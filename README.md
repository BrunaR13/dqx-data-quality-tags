# Data Quality com DQX + tags do Unity Catalog (2 times)

Exemplo simples de qualidade de dados no Databricks com [DQX](https://github.com/databrickslabs/dqx), seguindo a ideia de
**detectar e avisar sem bloquear**:

- as regras são escritas **uma vez** e ficam ligadas a **tags** do Unity Catalog, não a tabelas;
- o DQX **marca** as linhas com problema na silver (`_errors` / `_warnings`) sem descartar nenhuma;
- cada time dono dos dados vê os seus problemas num **dashboard** e recebe um **alerta**.

Tudo é implantado com Declarative Automation Bundles (DABs). Usa DQX 0.16.0 em jobs serverless.

## As 5 etapas

```
 ①  REGRAS             ②  BRONZE + TAGS          ③  SILVER (DQX)              ④  DASHBOARD      ⑤  ALERTAS
 regras_dq.yml   ──►  bronze_apolices   ──►   silver_apolices   ──►   dq_resultados ──►  1 dashboard   + 1 alerta por time
 "tag → regra"        (Time Emissão)          silver_sinistros        dq_resumo          filtro por time  (Emissão / Sinistros)
                      bronze_sinistros        todas as linhas,        dq_regras_aplicadas
                      (Time Sinistros)        marcadas em _errors/_warnings
```

| # | Etapa | Arquivo | O que mostrar |
|---|---|---|---|
| 1 | **Regras** | `regras_dq.yml` | Cada regra é ligada a uma tag (`"dq_tipo = cpf"`) e não a uma tabela. Os 2 times estão cadastrados. |
| 2 | **Bronze + tags** | `notebooks/01_bronze_e_tags.py` | Os dados de cada time chegando, alguns com erro. A catalogação é só `SET TAGS` (também dá pelo Catalog Explorer). |
| 3 | **Bronze → Silver** | `notebooks/02_bronze_para_silver.py` | Tags viram regras, o DQX aplica e **todas as linhas** vão para a silver, com `_errors` / `_warnings`. |
| 4 | **Dashboard** | `dashboards/dq_simples.lvdash.json` | Problemas por time, por regra, o detalhe por chave e o reuso das regras entre as tabelas. |
| 5 | **Alertas** | `resources/alerts.yml` | Um alerta por time, com a mesma consulta mudando só o filtro. *Run now* manda o e-mail com a tabela de problemas. |

## Os dois times e as tags

| Tabela | Time (`dq_time`) | Chave (`dq_chave`) | Tags de coluna |
|---|---|---|---|
| `bronze_apolices` | Emissão | `nr_apolice` | cpf_segurado (`obrigatorio`, `cpf`), email (`email`), produto (`dominio produto`), valor_premio (`valor_monetario`), dt_emissao (`data_passada`), uf (`dominio uf`) |
| `bronze_sinistros` | Sinistros | `nr_sinistro` | nr_apolice (`obrigatorio`), tipo_sinistro (`dominio tipo_sinistro`), valor_reclamado (`valor_monetario`), dt_ocorrencia (`data_passada`), uf_ocorrencia (`dominio uf`) |

As regras `obrigatorio`, `valor_positivo`, `data_nao_futura` e `uf_valida` foram escritas **uma vez** e valem
**nas duas tabelas, de times diferentes**. A seção de reuso no dashboard mostra isso.

## Resultado esperado

| Time | Linhas | Com erro | Só aviso | OK | Problemas |
|---|---|---|---|---|---|
| Emissão | 10 | 5 (AP003 CPF com máscara, AP005 CPF vazio, AP008 prêmio negativo, AP009 produto inválido, AP010 data futura) | 1 (AP006 e-mail) | 4 | 7 (AP010 também tem UF inválida) |
| Sinistros | 8 | 4 (SN002 sem apólice, SN004 valor zero, SN005 data futura, SN007 tipo inválido) | 1 (SN008 UF) | 3 | 5 |

## Como rodar

Pré-requisitos: workspace com Unity Catalog e serverless, [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)
configurada e permissão para criar schema no catálogo escolhido.

```bash
databricks bundle deploy --profile <seu-profile> --var catalog=<seu_catalogo>
databricks bundle run dq_simples --profile <seu-profile> --var catalog=<seu_catalogo>
```

| Variável | Padrão | O que é |
|---|---|---|
| `catalog` | `main` | catálogo onde o schema da demo é criado |
| `schema` | `dqx_demo_simples` | schema da demo (criado pelo notebook 01) |
| `warehouse_id` | warehouse chamado *Serverless Starter Warehouse* | SQL Warehouse do dashboard e dos alertas. Para usar outro: `--var warehouse_id=<id>` |

Os alertas vão para o e-mail de quem fez o deploy. Para alterar o dashboard, edite `scripts/gerar_dashboard.py` e rode
`python3 scripts/gerar_dashboard.py` antes do deploy.

Depois, abra o dashboard *"[dev …] DQX Simples - Qualidade por time"* e os dois alertas *"DQX Simples - … - Time Emissão / Sinistros"*
e clique em **Run now** (a agenda dos alertas fica pausada).

## Roteiro de apresentação (≈ 10 min)

1. **Regras**: abrir o `regras_dq.yml`. "A regra de CPF está escrita aqui uma vez. Ela não sabe em que tabela vai rodar."
2. **Bronze**: no notebook 01, mostrar os dados dos dois times (os erros estão comentados no código).
3. **Tags**: célula de catalogação. "Governança só diz *o que é* cada coluna. Ninguém escreveu regra para essa tabela."
   A última célula lista as tags; dá para mostrar as mesmas tags na UI do Catalog Explorer.
4. **Silver**: no notebook 02, mostrar a primeira tabela (tag → regra → coluna). Depois a silver: **nenhuma linha foi descartada**, e quem tem
   problema traz a mensagem em `erros` / `avisos`.
5. **Dashboard**: filtrar por time. Cada time vê só o que é dele e a lista de chaves a corrigir.
6. **Alertas**: *Run now* no alerta da Emissão: o e-mail chega com tabela, regra, quantidade e as chaves.
7. **Fechamento (tabela nova)**: "Se amanhã chega uma tabela nova, é só colocar as tags. O notebook 02 não muda."
   Ao vivo, dá para rodar `ALTER TABLE ... SET TAGS` numa tabela nova `bronze_*` do schema e rodar o job de novo.

Consultas úteis:

```sql
-- silver: tudo entra, só que marcado
SELECT nr_apolice, transform(_errors, e -> e.message) erros, transform(_warnings, w -> w.message) avisos
FROM <catalogo>.dqx_demo_simples.silver_apolices;

-- o que cada time precisa corrigir
SELECT time_responsavel, tabela, chave_registro, regra, criticidade, mensagem
FROM <catalogo>.dqx_demo_simples.dq_resultados ORDER BY 1, 2, 3;

-- reuso: mesma regra em várias tabelas
SELECT regra, collect_set(tabela) tabelas, count(*) colunas
FROM <catalogo>.dqx_demo_simples.dq_regras_aplicadas GROUP BY regra ORDER BY colunas DESC;
```

## Observações

- Tabela nova: o notebook 02 pega toda tabela do schema com a tag `dq_time`. A silver se chama `silver_<nome>`
  (troca o prefixo `bronze_`).
- Uma tag sem regra no YAML (ex.: `dq_tipo = placa`) ou um time não cadastrado fazem o notebook 02 falhar com uma mensagem clara.
- DQX 0.16+: em `is_in_list`, os literais precisam de aspas simples. O notebook coloca as aspas sozinho, por isso o YAML fica limpo.
- As tags ficam na **bronze**, porque a silver é recriada a cada execução. Em produção, com MERGE incremental, elas
  podem ficar na silver, como num pipeline incremental.
