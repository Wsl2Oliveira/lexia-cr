# LexIA — Carta-Resposta Judicial via IA

Automação de confecção de cartas-resposta judiciais, substituindo o fluxo UiPath por um pipeline Python puro.

## Arquitetura

```
Databricks (jud_athena_*)
    → Python consulta SQL direto
    → Para cada caso pendente:
        1. Waze API → shard do CPF
        2. Customers API → customer ID
        3. Crebito API → cartões ativos
        4. Rayquaza API → ativos disponíveis
        5. Petrificus API → bloqueios existentes
        6. Gemini (prompt LexIA) → decisão de macro (1-9)
        7. Google Docs API → copia template, preenche placeholders
        8. Google Drive API → salva PDF na pasta destino
```

## Dependências eliminadas

| Antes (UiPath)                    | Agora (Python)          |
|-----------------------------------|-------------------------|
| UiPath Orchestrator               | CLI `lexia run`         |
| Google Sheets (intermediário)     | Databricks SQL direto   |
| Apps Script                       | Google Docs/Drive API   |
| Oracle DB (RPA_CTRL_JUD_LEXIA)    | Databricks SQL          |
| Integration Service (Gemini)      | `google-generativeai`   |

## Setup

```bash
# 1. Instalar
pip install -e ".[dev]"

# 2. Configurar
cp .env.example .env
# Preencher .env com credenciais

# 3. Verificar configuração
lexia check

# 4. Dry-run (só lista casos sem processar)
lexia run --dry-run

# 5. Executar
lexia run --days 12
```

## Estrutura

```
src/lexia/
├── config.py              # Settings (env vars)
├── main.py                # CLI (typer)
├── orchestrator.py        # Pipeline principal
├── databricks/
│   └── query.py           # Query jud_athena_* tables
├── apis/
│   ├── auth.py            # Cert auth + uber token
│   ├── waze.py            # Find Shard (CPF → shard)
│   ├── customers.py       # Find Customer ID
│   ├── crebito.py         # Find Active Cards
│   ├── rayquaza.py        # Find Available Assets
│   └── petrificus.py      # Find Blocks
├── gemini/
│   └── prompt.py          # LexIA prompt + Gemini call
└── docs/
    └── generator.py       # Google Docs copy + fill + PDF export
```
