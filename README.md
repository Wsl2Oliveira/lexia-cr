# LexIA CR — Confecção de Carta-Resposta Judicial via IA

Pipeline autônomo que gera cartas-resposta para ofícios judiciais recebidos pela Nubank. Busca os casos pendentes, consulta os dados do cliente, aplica a resposta correta usando IA (Gemini) e salva a carta formatada no Google Drive — tudo sem intervenção manual.

---

## Como funciona (visão geral)

```
Databricks          APIs Nubank          LLM (Gemini)         Google Drive         Slack
  (casos)     →    (dados do cliente)  →  (decisão da IA)  →  (carta-resposta)  →  (notificação)
```

O pipeline roda em **5 fases** para cada caso:

1. **Busca de casos** — Consulta o Databricks para encontrar ofícios judiciais pendentes
2. **Enriquecimento** — Consulta 8 APIs internas da Nubank para obter dados do cliente
3. **Decisão da IA** — Envia os dados para o Gemini, que escolhe a macro de resposta correta
4. **Registro** — Salva o resultado completo (23+ campos) na planilha de rastreabilidade
5. **Geração da carta** — Cria o Google Doc formatado a partir do template oficial

---

## Quais casos são buscados?

A query do Databricks puxa ofícios judiciais dos últimos **3 dias** (configurável via `DAYS_BACK`) que atendam a todos estes critérios:

- Status: **confirmados** pela triagem do Athena (`official_letter_extraction_status__confirmed`)
- Tipos: **Bloqueio**, **Desbloqueio** e **Transferência**
- Com número de processo preenchido

### Tabelas consultadas

| Tabela | O que contém |
|--------|-------------|
| `etl.br__dataset.jud_athena_official_letter_extractions` | Dados extraídos do ofício (processo, vara, órgão, data) |
| `etl.br__dataset.jud_athena_submissions` | Submissões de ofícios |
| `etl.br__dataset.jud_athena_official_letters` | Tipo do ofício e informações investigadas |
| `etl.br__contract.jud_athena__investigated_information` | CPF/CNPJ, nome, valor solicitado, se é cliente |
| Tabelas PII (`_name_pii`, `_cpf_cnpj_pii`) | Decodificação dos hashes para dados reais |

A query deduplica automaticamente (se um mesmo processo aparece mais de uma vez, usa a triagem mais recente).

### Deduplicação de processamento

Antes de processar, o pipeline verifica na planilha de logs quais processos já foram finalizados com sucesso. Casos já processados são **pulados automaticamente**, evitando duplicação.

Cada execução gera um ID único (ex: `LX-7F3A-K9B2`) para rastreabilidade.

---

## Dados do cliente — quais APIs são consultadas?

Para cada caso, o pipeline consulta **8 APIs internas** usando certificados mTLS do `nucli`:

| API | O que busca |
|-----|-------------|
| **Waze** | Localiza o shard do cliente (PF via CPF, PJ via CNPJ) |
| **Customers** | Obtém o `customer_id` (PF ou PJ) |
| **savings-accounts** | Verifica se o cliente possui NuConta e obtém o ID |
| **Diablo** | Saldo disponível da NuConta (na data atual) |
| **Facade** | Cartões de crédito, status da conta crédito e faturas |
| **Rayquaza** | Ativos disponíveis (NuConta + Caixinhas + investimentos) |
| **Petrificus** | Bloqueios judiciais ativos (freeze-orders) e valores constritos |
| **Mario-Box** | Metadata das Caixinhas (nomes, quantidade) |
| **bank-accounts-widget-provider** | Agência e número da conta NuConta |

O saldo total bloqueável é calculado como: **NuConta disponível + Caixinhas** (Rayquaza `liquid_deposit`).

---

## Macros de resposta — como a IA decide?

A IA (Gemini 2.0 Flash via LiteLLM) recebe todos os dados do caso e escolhe a macro de resposta mais adequada. São **13 macros** divididas em 3 grupos:

### Bloqueio (Macros 2–9)

| Macro | Quando usar |
|-------|-------------|
| **2** — Conta inexistente | CPF/CNPJ não possui conta ativa |
| **3** — Saldo zerado/ínfimo | Saldo combinado < R$ 10,00 |
| **4** — Bloqueio padrão | Saldo >= R$ 10,00, sem bloqueio anterior |
| **5** — Bloqueio com anteriores | Saldo >= R$ 10,00, já existem bloqueios judiciais |
| **6** — Bloqueio total da conta | Ordem de bloqueio total da conta |
| **7** — Só tem cartão de crédito | Sem NuConta, sem conta crédito, com cartão |
| **8** — Bloqueio de cartão | Cartão de crédito bloqueado |
| **9** — Sem cartão de crédito | Não possui cartão de crédito |

### Desbloqueio (Macros 1 e 1B)

| Macro | Quando usar |
|-------|-------------|
| **1** — Desbloqueio realizado | Valores livres de bloqueio |
| **1B** — Desbloqueio parcial | Conta desbloqueada, mas valores permanecem constritos |

### Transferência (Macros T1–T3)

| Macro | Quando usar |
|-------|-------------|
| **T1** — Conta zerada | Saldo zero, transferência inviável |
| **T2** — Não é cliente | CPF/CNPJ não consta no cadastro |
| **T3** — Transferência viável | Saldo disponível, posição completa do cliente |

A IA recebe uma **sugestão pré-calculada** baseada em regras numéricas (saldo, existência de conta, bloqueios ativos). Isso garante consistência — a IA pode refinar o texto, mas a macro é determinada por dados concretos.

---

## Onde ficam as cartas geradas?

As cartas-resposta são salvas como **Google Docs** no Drive, dentro de uma pasta configurada via `GOOGLE_DRIVE_FOLDER_ID`.

### Estrutura no Drive

```
📁 [Pasta configurada]
  📁 0001179-61.2024.5.19.0005/
    📄 CR-0001179-61.2024.5.19.0005-BLOQUEIO
  📁 0807525-18.2022.8.12.0002/
    📄 CR-0807525-18.2022.8.12.0002-TRANSFERÊNCIA
  ...
```

Cada caso gera uma **subpasta com o número do processo**, contendo o Google Doc da carta-resposta. O nome do documento segue o padrão `CR-[processo]-[tipo]`.

O documento é gerado a partir de um **template do Google Docs** (configurado via `GOOGLE_TEMPLATE_DOC_ID`) usando substituição de variáveis via Apps Script:

- `{{data da elaboração deste documento}}` → data atual por extenso
- `{{número do ofício}}` → número do ofício judicial
- `{{número do processo}}` → número do processo
- `{{Vara/Seccional}}` → vara e tribunal
- `{{NOME DO CLIENTE ATINGIDO}}` → nome do investigado
- `{{documento do cliente atingido}}` → CPF/CNPJ formatado
- `{{macro da operação realizada}}` → texto de resposta gerado pela IA

---

## Notificações no Slack

O pipeline posta atualizações em tempo real em uma thread do Slack:

- **Início**: título da execução com data, total de casos e breakdown por tipo
- **Por caso**: resultado (sucesso/falha), processo, tipo, macro aplicada e link do doc
- **Final**: resumo com total de sucessos, erros e duração

Todas as execuções do mesmo dia ficam na **mesma thread** (o `thread_ts` é cacheado em `logs/.slack_thread_ts`).

---

## Execução agendada

O pipeline pode rodar automaticamente via `launchd` (macOS):

```bash
# Carregar o agendamento
launchctl load ~/Library/LaunchAgents/com.lexia.pipeline.plist

# Verificar se está ativo
launchctl list | grep lexia
```

O script `scripts/run_scheduled.sh` cuida de:
- Validar certificados mTLS (alerta no Slack se estiverem ausentes/expirados)
- Carregar o `.env`
- Ativar o virtualenv
- Executar o pipeline com log rotacionado (últimos 30 dias)

---

## Execução manual

```bash
# Ativar o ambiente
cd /caminho/para/lexia-cr
source .venv/bin/activate

# Rodar para processos específicos
LEXIA_TARGET_PROCESSES="0001179-61.2024.5.19.0005,0807525-18.2022.8.12.0002" \
  PYTHONPATH=src python scripts/run_traced_pipeline.py

# Rodar para os últimos 12 dias (sem filtro)
PYTHONPATH=src python scripts/run_traced_pipeline.py

# Limitar quantidade de casos
LEXIA_LIMIT=10 PYTHONPATH=src python scripts/run_traced_pipeline.py
```

### Reprocessar um caso já finalizado

Se precisar reprocessar um caso que já foi marcado como `success` na planilha:
1. Abra a planilha de logs
2. Localize a linha do processo
3. Altere a coluna `status_execucao` de `success` para qualquer outro valor (ex: `reprocessar`)
4. Execute o pipeline novamente — o caso será incluído

---

## Configuração

Copie o `.env.example` para `.env` e preencha:

```bash
cp .env.example .env
```

| Variável | Descrição |
|----------|-----------|
| `DATABRICKS_HOST` | Host do Databricks |
| `DATABRICKS_TOKEN` | Token de acesso (PAT) |
| `DATABRICKS_HTTP_PATH` | Caminho do SQL Warehouse |
| `APPS_SCRIPT_URL` | URL do deploy do Apps Script |
| `GOOGLE_TEMPLATE_DOC_ID` | ID do template de carta-resposta no Drive |
| `GOOGLE_DRIVE_FOLDER_ID` | ID da pasta de destino no Drive |
| `GOOGLE_SERVICE_ACCOUNT_PATH` | Caminho do JSON da service account |
| `LITELLM_API_KEY` | Chave de API do LiteLLM |
| `LITELLM_BASE_URL` | URL base do proxy LiteLLM |
| `LITELLM_MODEL` | Modelo da LLM (padrão: `gemini/gemini-2.0-flash`) |
| `SLACK_BOT_TOKEN` | Token do bot do Slack (scope: `chat:write`) |
| `SLACK_CHANNEL_ID` | ID do canal do Slack |
| `LEXIA_SPREADSHEET_ID` | ID da planilha de rastreabilidade |
| `DAYS_BACK` | Dias retroativos da query (padrão: 3) |

### Pré-requisitos

- Python 3.11+
- `nucli` autenticado (para certificados mTLS das APIs internas)
- Service account do Google com acesso à planilha e ao Drive
- Bot do Slack com scope `chat:write` instalado no canal

```bash
# Instalar dependências
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

---

## Estrutura do projeto

```
lexia-cr/
├── scripts/
│   ├── run_traced_pipeline.py    # Pipeline principal (5 fases)
│   ├── run_scheduled.sh          # Wrapper para execução agendada
│   └── update_oficio_links.py    # Atualiza links dos PDFs na planilha
├── apps-script/
│   ├── Código.js                 # Apps Script (geração de docs e busca no Drive)
│   └── appsscript.json           # Configuração do Apps Script
├── src/lexia/
│   ├── config.py                 # Configuração centralizada (.env)
│   ├── gemini/prompt.py          # Prompt do LLM (referência, não usado pelo pipeline)
│   └── ...
├── notebooks/
│   └── lexia_casos_pendentes.sql # Query de referência para o Databricks
├── data/examples/                # Exemplos de cartas (não versionado — PII)
│   ├── bloqueio/
│   ├── desbloqueio/
│   └── transferencia/
├── docs/
│   └── report-lexia-cr-abril-2026.md
├── .env.example                  # Template de configuração
└── pyproject.toml                # Dependências Python
```

---

## Diferenças em relação ao fluxo anterior (UiPath)

| Aspecto | Antes (UiPath) | Agora (LexIA) |
|---------|----------------|---------------|
| Dependência | UiPath + licença + banco Oracle JD | Python + Databricks direto |
| Consulta do cliente | 5 APIs (Waze, Customers, Crebito, Rayquaza, Petrificus) | 8 APIs (+Diablo, Facade, savings-accounts, Mario-Box, bank-accounts-widget-provider) |
| Decisão da resposta | Analista escolhe manualmente | IA escolhe com base em regras + dados |
| Geração da carta | Analista redige | IA gera o texto, doc formatado automaticamente |
| Rastreabilidade | Bot Thunder | Planilha com 23+ campos por caso + Slack |
| Autonomia | Depende de VDI e operador | Roda sozinho (launchd) |
| Tempo por caso | ~19 min (bloqueio), ~13 min (desbloqueio) | ~2-3 min por caso |

---

## Repositório e reconstrução do ambiente

Todo o código-fonte, configurações, scripts, testes e CI estão versionados neste repositório GitHub. Para reconstruir o ambiente de execução completo a partir do zero:

```bash
git clone https://github.com/Wsl2Oliveira/lexia-cr.git
cd lexia-cr
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
# Preencher as variáveis no .env
```

Arquivos **não versionados** (por segurança ou por serem efêmeros):

| Arquivo / Pasta | Motivo | Como obter |
|-----------------|--------|------------|
| `.env` | Credenciais reais | Copiar de `.env.example` e preencher |
| `data/examples/` | Contém PII fictício para calibração | Regenerar via `scripts/generate_examples.py` |
| `logs/` | Logs de execução | Gerados automaticamente a cada run |
| `tmp/` | PDFs temporários de ofícios | Gerados durante processamento |
| `.drive_export/` | Exports de análise exploratória | Não necessários para operação |
| Certificados mTLS (`.pem`) | Credenciais de API | `nucli` gera automaticamente |
