# Databricks notebook source
# MAGIC %md
# MAGIC # ETAPA 2 · Bronze chegando + catalogação com tags
# MAGIC
# MAGIC Dois times enviam dados para a bronze:
# MAGIC
# MAGIC | Tabela | Time dono | Chave |
# MAGIC |---|---|---|
# MAGIC | `bronze_apolices` | **Emissão** | `nr_apolice` |
# MAGIC | `bronze_sinistros` | **Sinistros** | `nr_sinistro` |
# MAGIC
# MAGIC Os dados são poucos e escritos à mão, para dar para ler tudo na tela. Alguns registros chegam com erro de propósito.
# MAGIC
# MAGIC Depois, a governança **cataloga** as tabelas com tags. Nenhuma regra é escrita aqui: as tags dizem
# MAGIC *o que é cada coluna*, e o arquivo `regras_dq.yml` diz *como validar cada tag*.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dqx_demo_simples")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
s = f"{catalog}.{schema}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s} COMMENT 'Demo simplificada - Data Quality com DQX e tags'")

# COMMAND ----------

# MAGIC %md ## Bronze: o que chega dos sistemas de origem

# COMMAND ----------

from datetime import date, timedelta
from decimal import Decimal as D

futuro = date.today() + timedelta(days=30)

apolices = [
    # nr_apolice, cpf_segurado,      email,                 produto,            valor_premio, dt_emissao,          uf
    ("AP001", "12345678901",    "ana@email.com",       "VIDA_INDIVIDUAL",  D("120.00"),  date(2026, 9, 1),    "SP"),
    ("AP002", "23456789012",    "bruno@email.com",     "VIDA_GRUPO",       D("89.90"),   date(2026, 9, 3),    "RJ"),
    ("AP003", "345.678.901-23", "carla@email.com",     "PREVIDENCIA_PGBL", D("500.00"),  date(2026, 9, 5),    "MG"),  # CPF com máscara
    ("AP004", "45678901234",    "diego@email.com",     "PREVIDENCIA_VGBL", D("750.00"),  date(2026, 9, 8),    "RS"),
    ("AP005", None,             "elisa@email.com",     "VIDA_INDIVIDUAL",  D("110.00"),  date(2026, 9, 10),   "PR"),  # CPF vazio
    ("AP006", "67890123456",    "fabio@email",         "VIDA_GRUPO",       D("95.00"),   date(2026, 9, 12),   "SC"),  # e-mail inválido (aviso)
    ("AP007", "78901234567",    "gabi@email.com",      "VIDA_INDIVIDUAL",  D("130.00"),  date(2026, 9, 15),   "BA"),
    ("AP008", "89012345678",    "hugo@email.com",      "PREVIDENCIA_PGBL", D("-150.00"), date(2026, 9, 18),   "PE"),  # prêmio negativo
    ("AP009", "90123456789",    "iris@email.com",      "VIDA_XPTO",        D("200.00"),  date(2026, 9, 20),   "CE"),  # produto fora do domínio
    ("AP010", "01234567890",    "joao@email.com",      "VIDA_GRUPO",       D("99.00"),   futuro,              "XX"),  # data futura + UF inválida (aviso)
]
spark.createDataFrame(
    apolices,
    "nr_apolice string, cpf_segurado string, email string, produto string, valor_premio decimal(12,2), dt_emissao date, uf string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{s}.bronze_apolices")

sinistros = [
    # nr_sinistro, nr_apolice, tipo_sinistro,  valor_reclamado,  dt_ocorrencia,       uf_ocorrencia
    ("SN001", "AP001", "MORTE",         D("100000.00"), date(2026, 10, 1),   "SP"),
    ("SN002", None,    "INVALIDEZ",     D("50000.00"),  date(2026, 10, 3),   "RJ"),   # sem apólice
    ("SN003", "AP004", "DOENCA_GRAVE",  D("80000.00"),  date(2026, 10, 5),   "RS"),
    ("SN004", "AP002", "FUNERAL",       D("0.00"),      date(2026, 10, 7),   "RJ"),   # valor zerado
    ("SN005", "AP007", "MORTE",         D("120000.00"), futuro,              "BA"),   # ocorrência no futuro
    ("SN006", "AP006", "INVALIDEZ",     D("30000.00"),  date(2026, 10, 12),  "SC"),
    ("SN007", "AP003", "ROUBO",         D("5000.00"),   date(2026, 10, 15),  "MG"),   # tipo fora do domínio
    ("SN008", "AP009", "FUNERAL",       D("7000.00"),   date(2026, 10, 18),  "SPP"),  # UF inválida (aviso)
]
spark.createDataFrame(
    sinistros,
    "nr_sinistro string, nr_apolice string, tipo_sinistro string, valor_reclamado decimal(14,2), dt_ocorrencia date, uf_ocorrencia string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{s}.bronze_sinistros")

display(spark.table(f"{s}.bronze_apolices"))
display(spark.table(f"{s}.bronze_sinistros"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalogação com tags
# MAGIC
# MAGIC Dá para fazer pela UI do **Catalog Explorer** ou por SQL, como aqui.
# MAGIC
# MAGIC - **Tags de tabela**: `dq_time` (time dono) e `dq_chave` (chave do registro).
# MAGIC - **Tags de coluna**: o que o dado é. Cada tag puxa as regras dela no `regras_dq.yml`.
# MAGIC
# MAGIC Repare que as duas tabelas usam várias **tags iguais** (`dq_obrigatorio`, `dq_tipo = valor_monetario`,
# MAGIC `dq_tipo = data_passada`, `dq_dominio = uf`). A regra foi escrita uma vez e vale nos dois times.

# COMMAND ----------

ap = f"{s}.bronze_apolices"
spark.sql(f"ALTER TABLE {ap} SET TAGS ('dq_time' = 'Emissão', 'dq_chave' = 'nr_apolice')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN cpf_segurado SET TAGS ('dq_obrigatorio' = 'sim', 'dq_tipo' = 'cpf')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN email        SET TAGS ('dq_tipo' = 'email')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN produto      SET TAGS ('dq_dominio' = 'produto')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN valor_premio SET TAGS ('dq_tipo' = 'valor_monetario')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN dt_emissao   SET TAGS ('dq_tipo' = 'data_passada')")
spark.sql(f"ALTER TABLE {ap} ALTER COLUMN uf           SET TAGS ('dq_dominio' = 'uf')")

sn = f"{s}.bronze_sinistros"
spark.sql(f"ALTER TABLE {sn} SET TAGS ('dq_time' = 'Sinistros', 'dq_chave' = 'nr_sinistro')")
spark.sql(f"ALTER TABLE {sn} ALTER COLUMN nr_apolice      SET TAGS ('dq_obrigatorio' = 'sim')")
spark.sql(f"ALTER TABLE {sn} ALTER COLUMN tipo_sinistro   SET TAGS ('dq_dominio' = 'tipo_sinistro')")
spark.sql(f"ALTER TABLE {sn} ALTER COLUMN valor_reclamado SET TAGS ('dq_tipo' = 'valor_monetario')")
spark.sql(f"ALTER TABLE {sn} ALTER COLUMN dt_ocorrencia   SET TAGS ('dq_tipo' = 'data_passada')")
spark.sql(f"ALTER TABLE {sn} ALTER COLUMN uf_ocorrencia   SET TAGS ('dq_dominio' = 'uf')")

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name AS tabela, column_name AS coluna, tag_name AS tag, tag_value AS valor
    FROM {catalog}.information_schema.column_tags
    WHERE schema_name = '{schema}' AND startswith(tag_name, 'dq_')
    ORDER BY tag, valor, tabela
"""))
