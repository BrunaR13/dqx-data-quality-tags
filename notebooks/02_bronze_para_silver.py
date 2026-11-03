# Databricks notebook source
# MAGIC %md
# MAGIC # ETAPA 3 · Bronze → Silver com DQX (marca, não bloqueia)
# MAGIC
# MAGIC Para cada tabela catalogada com `dq_time`:
# MAGIC
# MAGIC 1. **Tags → regras**: lê as tags no `information_schema` e pega no `regras_dq.yml` as regras de cada tag.
# MAGIC 2. **DQX**: aplica as regras. **Todas as linhas seguem para a silver**; as que têm problema ganham
# MAGIC    as colunas `_errors` e `_warnings`, com a mensagem e o time responsável.
# MAGIC 3. **Resultados**: grava uma linha por registro × regra com problema em `dq_resultados`.
# MAGIC    É essa tabela que alimenta o **dashboard** e os **alertas** de cada time.
# MAGIC
# MAGIC O notebook não conhece nenhuma tabela pelo nome. Tabela nova com tags entra sozinha na próxima execução.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.16.0

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dqx_demo_simples")
dbutils.widgets.text("arquivo_regras", "../regras_dq.yml")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
arquivo_regras = dbutils.widgets.get("arquivo_regras")
s = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md ## 1 · Tags → regras

# COMMAND ----------

import json

import yaml

with open(arquivo_regras, encoding="utf-8") as f:
    config = yaml.safe_load(f)

tags_tabela = spark.sql(f"""
    SELECT table_name, tag_name, tag_value FROM {catalog}.information_schema.table_tags
    WHERE schema_name = '{schema}' AND tag_name IN ('dq_time', 'dq_chave')
""").collect()
tags_coluna = spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags
    WHERE schema_name = '{schema}' AND startswith(tag_name, 'dq_')
    ORDER BY table_name, column_name, tag_name
""").collect()

tabelas = {}
for t in tags_tabela:
    tabelas.setdefault(t.table_name, {})[t.tag_name] = t.tag_value


def montar_regra(regra: dict, coluna: str, time: str) -> dict:
    """Troca {coluna} pelo nome real da coluna e anexa o time responsável à regra."""
    texto = json.dumps(regra, ensure_ascii=False).replace("{coluna}", coluna)
    r = json.loads(texto)
    if r["check"]["function"] == "is_in_list":  # DQX 0.16+: literais da lista precisam de aspas simples
        r["check"]["arguments"]["allowed"] = [f"'{v}'" for v in r["check"]["arguments"]["allowed"]]
    return {
        "name": f"{r['name']}__{coluna}",
        "criticality": r["criticality"],
        "check": r["check"],
        **({"filter": r["filter"]} if r.get("filter") else {}),
        "user_metadata": {"time": time, "coluna": coluna, "regra_base": r["name"]},
    }


regras_por_tabela, regras_aplicadas = {}, []
for tabela, tt in sorted(tabelas.items()):
    time = tt["dq_time"]
    assert time in config["times"], f"{tabela}: time '{time}' não está cadastrado no regras_dq.yml"
    checks = []
    for c in [c for c in tags_coluna if c.table_name == tabela]:
        chave_tag = f"{c.tag_name} = {c.tag_value}"
        assert chave_tag in config["regras"], f"{tabela}.{c.column_name}: a tag '{chave_tag}' não tem regra"
        for regra in config["regras"][chave_tag]:
            check = montar_regra(regra, c.column_name, time)
            checks.append(check)
            regras_aplicadas.append((time, tabela, c.column_name, chave_tag, regra["name"], check["name"], regra["criticality"]))
    regras_por_tabela[tabela] = checks

spark.createDataFrame(
    regras_aplicadas, "time_responsavel string, tabela string, coluna string, tag string, regra string, regra_coluna string, criticidade string"
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{s}.dq_regras_aplicadas")

display(spark.table(f"{s}.dq_regras_aplicadas").orderBy("regra", "tabela"))

# COMMAND ----------

# MAGIC %md ## 2 · DQX: todas as linhas vão para a silver, marcadas

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

dq = DQEngine(WorkspaceClient())
resultados, resumo = None, []

for tabela, checks in regras_por_tabela.items():
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors, status.errors

    chave = tabelas[tabela]["dq_chave"]
    time = tabelas[tabela]["dq_time"]
    silver = f"{s}.{tabela.replace('bronze_', 'silver_', 1)}"

    marcado = dq.apply_checks_by_metadata(spark.table(f"{s}.{tabela}"), checks)
    marcado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver)
    df = spark.table(silver)

    # 3 · uma linha por registro x regra com problema
    for col_resultado in ["_errors", "_warnings"]:
        parte = df.select(F.col(chave).alias("chave_registro"), F.explode(col_resultado).alias("r")).select(
            F.lit(time).alias("time_responsavel"),
            F.lit(silver.split(".")[-1]).alias("tabela"),
            "chave_registro",
            F.col("r.name").alias("regra"),
            F.col("r.user_metadata")["coluna"].alias("coluna"),
            F.lit("error" if col_resultado == "_errors" else "warn").alias("criticidade"),
            F.col("r.message").alias("mensagem"),
        )
        resultados = parte if resultados is None else resultados.unionByName(parte)

    m = df.agg(
        F.count("*").alias("linhas"),
        F.sum((F.size("_errors") > 0).cast("int")).alias("com_erro"),
        F.sum(((F.size("_warnings") > 0) & (F.coalesce(F.size("_errors"), F.lit(0)) <= 0)).cast("int")).alias("so_aviso"),
    ).first()
    resumo.append((time, silver.split(".")[-1], m.linhas, m.com_erro or 0, m.so_aviso or 0,
                   m.linhas - (m.com_erro or 0) - (m.so_aviso or 0)))
    print(f"{silver}: {m.linhas} linhas | {m.com_erro or 0} com erro | {m.so_aviso or 0} só com aviso")

# COMMAND ----------

(resultados.withColumn("executado_em", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{s}.dq_resultados"))

(spark.createDataFrame(resumo, "time_responsavel string, tabela string, linhas bigint, linhas_com_erro bigint, "
                                "linhas_so_com_aviso bigint, linhas_ok bigint")
 .withColumn("executado_em", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{s}.dq_resumo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como fica a silver
# MAGIC Nenhuma linha foi descartada. Quem tem problema está marcado em `_errors` (erro) ou `_warnings` (aviso).

# COMMAND ----------

display(spark.sql(f"""
    SELECT nr_apolice, cpf_segurado, email, produto, valor_premio, uf,
           transform(_errors, e -> e.message) AS erros, transform(_warnings, w -> w.message) AS avisos
    FROM {s}.silver_apolices ORDER BY nr_apolice
"""))

# COMMAND ----------

display(spark.table(f"{s}.dq_resultados").orderBy("time_responsavel", "tabela", "chave_registro"))
