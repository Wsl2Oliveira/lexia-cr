# LexIA — Carta-Resposta Judicial via IA

Automação de confecção de cartas-resposta judiciais, substituindo o fluxo UiPath por um pipeline Python puro.

## Arquitetura

```
Databricks (jud_athena_*)
    → Python consulta SQL direto
    → Para cada caso pendente:
        1. Waze API → shard do CPF
        2. Facade API → conta crédito + cartões + saldos
        3. Rayquaza API → ativos disponíveis
        4. Petrificus API → bloqueios judiciais existentes
        5. LLM (LiteLLM) → decisão de macro (1-10)
        6. Apps Script Web App → copia template, preenche placeholders
        7. Google Drive → salva Google Doc na pasta destino
        8. Google Sheets → log de rastreabilidade
```

## Pipeline de Execução

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐    ┌───────────┐
│  Databricks │───▶│  APIs Nubank │───▶│  LLM Macro  │───▶│ Apps Script  │───▶│  Sheets   │
│  (casos)    │    │  (enrich)    │    │  (decisão)  │    │ (doc Drive)  │    │  (logs)   │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘    └───────────┘
     Fase 1             Fase 2             Fase 3             Fase 5            Fase 4
```

## Dependências eliminadas

| Antes (UiPath)                    | Agora (Python)                |
|-----------------------------------|-------------------------------|
| UiPath Orchestrator               | `python scripts/run_traced_pipeline.py` |
| Google Sheets (intermediário)     | Databricks SQL direto         |
| Apps Script (manual)              | Apps Script Web App (HTTP)    |
| Oracle DB (RPA_CTRL_JUD_LEXIA)    | Databricks SQL                |
| Integration Service (Gemini)      | LiteLLM proxy (`openai` SDK)  |
| Crebito API (cards only)          | Facade API (conta + cards)    |

## Setup

```bash
# 1. Instalar
pip install -e ".[dev]"

# 2. Configurar
cp .env.example .env
# Preencher .env com credenciais

# 3. Deploy do Apps Script
# Copiar apps-script/Code.gs para Google Apps Script
# Deploy como Web App e copiar URL para APPS_SCRIPT_URL no .env

# 4. Executar pipeline
python scripts/run_traced_pipeline.py
```

## Estrutura

```
src/lexia/
├── config.py              # Settings (env vars via pydantic-settings)
├── main.py                # CLI (typer)
├── orchestrator.py        # Pipeline principal
├── databricks/
│   └── query.py           # Query jud_athena_* tables
├── apis/
│   ├── auth.py            # mTLS cert auth (nucli)
│   ├── waze.py            # CPF → shard mapping
│   ├── customers.py       # CPF → customer ID
│   ├── crebito.py         # Credit account cards
│   ├── rayquaza.py        # Available assets/balances
│   └── petrificus.py      # Judicial freeze orders
├── gemini/
│   └── prompt.py          # LLM prompt + LiteLLM call
└── docs/
    └── generator.py       # Apps Script Web App caller

apps-script/
└── Code.gs                # Google Apps Script (template copy + fill)

scripts/
├── run_traced_pipeline.py # Pipeline completo com rastreabilidade
└── generate_examples.py   # Gerador de cartas-exemplo

notebooks/
└── lexia_casos_pendentes.sql  # Query Databricks para casos pendentes

data/
└── slack_alpha_rpa_thread_index.json  # Índice de threads RPA (referência)
```

## Macros Disponíveis

### Bloqueio
| ID | Macro | Descrição |
|----|-------|-----------|
| 1 | `bloqueio_conta_bloqueada` | Conta existe, bloqueio realizado |
| 2 | `bloqueio_inexiste_conta` | CPF/CNPJ não possui conta ativa |
| 3 | `bloqueio_conta_zerada` | Conta zerada, bloqueio prejudicado |
| 4 | `bloqueio_saldo_irrisorio_bacenjud` | Saldo ≤ R$10, art. 13 §10 Bacenjud 2.0 |
| 5 | `bloqueio_cnpj_nao_cadastrado` | CNPJ não consta no cadastro |
| 6 | `bloqueio_conta_pagamentos_explicacao` | Esclarecer conta de pagamentos |
| 7 | `bloqueio_judicial_instaurado` | Bloqueio judicial ativo na conta |
| 8 | `bloqueio_sem_portabilidade_salario` | Sem portabilidade de salário |
| 9 | `bloqueio_monitoramento_recebiveis` | Monitoramento + Teimosinha |

### Desbloqueio
| ID | Macro | Descrição |
|----|-------|-----------|
| 10 | `desbloqueio_produtos_livres` | Bloqueios judiciais encerrados |
