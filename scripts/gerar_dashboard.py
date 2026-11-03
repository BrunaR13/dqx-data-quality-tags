"""Gera dashboards/dq_simples.lvdash.json (AI/BI). Rodar: python3 scripts/gerar_dashboard.py"""
import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "dashboards" / "dq_simples.lvdash.json"

COR_CRIT = [{"value": "error", "color": "#E15759"}, {"value": "warn", "color": "#F2B134"}]

datasets = [
    {"name": "ds_resultados", "displayName": "Problemas encontrados",
     "queryLines": ["SELECT time_responsavel, tabela, chave_registro, regra, coluna, criticidade, mensagem, executado_em ",
                    "FROM dq_resultados"]},
    {"name": "ds_resumo", "displayName": "Resumo por tabela",
     "queryLines": ["SELECT time_responsavel, tabela, linhas, linhas_com_erro, linhas_so_com_aviso, linhas_ok, executado_em ",
                    "FROM dq_resumo"],
     "columns": [{"displayName": "% linhas sem erro", "description": "linhas sem _errors / total de linhas",
                  "expression": "1 - SUM(`linhas_com_erro`) / SUM(`linhas`)"}]},
    {"name": "ds_regras", "displayName": "Regras aplicadas (vindas das tags)",
     "queryLines": ["SELECT time_responsavel, tabela, coluna, tag, regra, criticidade FROM dq_regras_aplicadas"]},
]

F = lambda n: {"name": n, "expression": f"`{n}`"}
S = lambda n: {"name": f"sum({n})", "expression": f"SUM(`{n}`)"}
CNT = {"name": "count(chave_registro)", "expression": "COUNT(`chave_registro`)"}
CNT_COL = {"name": "count(coluna)", "expression": "COUNT(`coluna`)"}
CAT = {"type": "categorical"}
QNT = {"type": "quantitative"}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": pos(x, y, w, h)}


def filtro(name, field, title, x, y, w, datasets_):
    """Filtro que vale para vários datasets ao mesmo tempo (um query por dataset)."""
    queries = [{"name": f"{name}_{d}", "query": {"datasetName": d, "fields": [F(field)], "disaggregated": False}}
               for d in datasets_]
    return {"widget": {
        "name": name, "queries": queries,
        "spec": {"version": 2, "widgetType": "filter-multi-select",
                 "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": q["name"]} for q in queries]},
                 "frame": {"showTitle": True, "title": title}}},
        "position": pos(x, y, w, 2)}


def counter(name, title, ds, field, x, y, desc, fmt=None):
    value = {"fieldName": field["name"], "displayName": title, **({"format": fmt} if fmt else {})}
    return {"widget": {
        "name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [field], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                 "frame": {"showTitle": True, "title": title, "description": desc, "showDescription": True}}},
        "position": pos(x, y, 3, 3)}


def bar(name, title, ds, x_enc, y_enc, color, fields, x, y, w, h, desc=None, stack=True):
    spec = {"version": 3, "widgetType": "bar",
            "encodings": {"x": x_enc, "y": y_enc, "color": color},
            "frame": {"showTitle": True, "title": title, **({"description": desc, "showDescription": True} if desc else {})}}
    if not stack:
        spec["mark"] = {"layout": "group"}
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": False}}],
                       "spec": spec},
            "position": pos(x, y, w, h)}


def tabela(name, title, ds, cols, x, y, w, h):
    return {"widget": {
        "name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [F(c) for c, _ in cols], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]},
                 "frame": {"showTitle": True, "title": title}}},
        "position": pos(x, y, w, h)}


TODOS = ["ds_resultados", "ds_resumo", "ds_regras"]

layout = [
    text("titulo", ["# Qualidade de dados por time\n", "\n",
                    "Regras ligadas a **tags** do Unity Catalog → DQX marca a silver (**sem bloquear**) → ",
                    "cada time vê aqui e recebe alerta dos problemas dos seus dados."], 0, 0, 12, 2),
    filtro("f_time", "time_responsavel", "Time", 0, 2, 6, TODOS),
    filtro("f_tabela", "tabela", "Tabela", 6, 2, 6, ["ds_resultados", "ds_resumo"]),

    counter("k_linhas", "Linhas na silver", "ds_resumo", S("linhas"), 0, 4, "nenhuma linha foi descartada"),
    counter("k_erro", "Linhas com erro", "ds_resumo", S("linhas_com_erro"), 3, 4, "marcadas em _errors"),
    counter("k_aviso", "Linhas só com aviso", "ds_resumo", S("linhas_so_com_aviso"), 6, 4, "marcadas em _warnings"),
    counter("k_pct", "% linhas sem erro", "ds_resumo",
            {"name": "measure(% linhas sem erro)", "expression": "MEASURE(`% linhas sem erro`)"}, 9, 4,
            "linhas sem _errors / total", PCT),

    bar("b_time", "Problemas por time", "ds_resultados",
        {"fieldName": "time_responsavel", "scale": CAT, "displayName": "Time"},
        {"fieldName": "count(chave_registro)", "scale": QNT, "displayName": "Problemas"},
        {"fieldName": "criticidade", "scale": {"type": "categorical", "mappings": COR_CRIT}, "displayName": "Criticidade"},
        [F("time_responsavel"), F("criticidade"), CNT], 0, 7, 5, 6, desc="error = prioridade alta · warn = aviso"),
    bar("b_regra", "Problemas por regra", "ds_resultados",
        {"fieldName": "count(chave_registro)", "scale": QNT, "displayName": "Problemas"},
        {"fieldName": "regra", "scale": CAT, "displayName": "Regra"},
        {"fieldName": "time_responsavel", "scale": CAT, "displayName": "Time"},
        [F("regra"), F("time_responsavel"), CNT], 5, 7, 7, 6),

    text("sec_detalhe", ["## O que corrigir"], 0, 13, 12, 1),
    tabela("t_detalhe", "Registros com problema (enviar ao time de origem)", "ds_resultados",
           [("time_responsavel", "Time"), ("tabela", "Tabela"), ("chave_registro", "Chave"), ("criticidade", "Criticidade"),
            ("regra", "Regra"), ("mensagem", "Mensagem do DQX")], 0, 14, 12, 7),

    text("sec_regras", ["## Reuso das regras: uma tag, várias tabelas\n",
                        "Cada regra foi escrita **uma vez** no `regras_dq.yml` e vale em toda coluna que recebeu a tag."],
         0, 21, 12, 2),
    bar("b_reuso", "Em quantas colunas cada regra foi aplicada", "ds_regras",
        {"fieldName": "count(coluna)", "scale": QNT, "displayName": "Colunas"},
        {"fieldName": "regra", "scale": CAT, "displayName": "Regra"},
        {"fieldName": "time_responsavel", "scale": CAT, "displayName": "Time"},
        [F("regra"), F("time_responsavel"), CNT_COL], 0, 23, 5, 7),
    tabela("t_regras", "Tag → regra → coluna", "ds_regras",
           [("tag", "Tag"), ("regra", "Regra"), ("criticidade", "Criticidade"), ("time_responsavel", "Time"),
            ("tabela", "Tabela"), ("coluna", "Coluna")], 5, 23, 7, 7),
]

dashboard = {
    "datasets": datasets,
    "pages": [{"name": "visao_geral", "displayName": "Visão geral", "pageType": "PAGE_TYPE_CANVAS",
               "layoutVersion": "GRID_V1", "layout": layout}],
    "uiSettings": {"theme": {"widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 8}},
}

OUT.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2))
print(f"gerado {OUT}")
