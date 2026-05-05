#!/usr/bin/env bash
# LexIA CR — Auto-reprocessamento de casos com falha transiente
#
# Lê o run-summary do dia atual, identifica casos que falharam em
# categorias de erro consideradas transientes (Drive instável, LLM 5xx,
# API enrichment intermitente), e dispara um único retry chamando o
# pipeline regular com LEXIA_TARGET_PROCESSES.
#
# Comportamento:
#   - Se não há summary do dia → exit 0 silencioso (rodagem principal não rodou)
#   - Se 0 casos transientes → exit 0 silencioso
#   - Se >50% dos casos da rodagem original falharam → exit 0 com aviso
#     (incidente real, precisa intervenção humana)
#   - Se a rodagem principal ainda está em curso (lock) → exit 0 silencioso
#
# Pensado para rodar 30 min após a rodagem principal via launchd.
#
# Categorias retentadas (transientes):
#   DOC_GENERATION    — Drive flap, retry quase sempre resolve
#   LLM_TRANSIENT     — timeout/5xx do provider de LLM
#   ENRICHMENT_API    — Waze/Customers/Facade intermitente
#   LLM_RATE_LIMIT    — janela do provider pode ter aberto
#   SHEETS            — gravação na planilha falhou pontualmente
#
# Categorias NÃO retentadas (estruturais, exigem humano):
#   CERTS_MISSING     — precisa renovar certs com `nucli`
#   DATABRICKS        — query/conectividade quebrada
#   LLM_PARSE         — provavelmente bug no prompt
#   UNKNOWN           — sem padrão conhecido

set -euo pipefail

LEXIA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$LEXIA_DIR/logs"
VENV="$LEXIA_DIR/.venv/bin/activate"
ENV_FILE="$LEXIA_DIR/.env"
LOCK_FILE="$LOG_DIR/.pipeline.lock"

mkdir -p "$LOG_DIR"

TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/auto-retry-$TODAY.log"
SUMMARY_FILE="$LOG_DIR/run-summary-$TODAY.json"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "========================================"
echo "[LexIA Retry] Auto-reprocessamento — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

# ── 1. Não roda se a principal estiver em curso ──
if [ -f "$LOCK_FILE" ]; then
    lock_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
        echo "[SKIP] Pipeline principal ainda em execução (PID $lock_pid)."
        exit 0
    fi
fi

# ── 2. Summary do dia precisa existir ──
if [ ! -f "$SUMMARY_FILE" ]; then
    echo "[SKIP] $SUMMARY_FILE não existe — rodagem principal não executou hoje."
    exit 0
fi

# ── 3. Cap de segurança: >50% erro = incidente real, NÃO retry ──
total=$(jq -r '.summary.totals.total // 0' "$SUMMARY_FILE")
errors=$(jq -r '.summary.totals.errors // 0' "$SUMMARY_FILE")

if [ "$total" -eq 0 ]; then
    echo "[SKIP] Rodagem do dia teve 0 casos."
    exit 0
fi

# Bash não faz aritmética float; usa awk
err_ratio=$(awk -v e="$errors" -v t="$total" 'BEGIN { printf "%.4f", e/t }')
err_pct=$(awk -v e="$errors" -v t="$total" 'BEGIN { printf "%.1f", (e/t)*100 }')

threshold_check=$(awk -v r="$err_ratio" 'BEGIN { print (r > 0.5) ? "1" : "0" }')
if [ "$threshold_check" = "1" ]; then
    echo "[ABORT] Taxa de erro $err_pct% > 50% — incidente real, exige intervenção humana."
    echo "         Não acionando auto-retry para evitar mascarar problema sistêmico."

    # Notifica no Slack que o auto-retry foi inibido
    source "$ENV_FILE" 2>/dev/null || true
    if [ -n "${SLACK_BOT_TOKEN:-}" ] && [ -n "${SLACK_CHANNEL_ID:-}" ]; then
        thread_ts=""
        if [ -f "$LOG_DIR/.slack_thread_ts" ]; then
            thread_ts=$(jq -r '.thread_ts // ""' "$LOG_DIR/.slack_thread_ts" 2>/dev/null || echo "")
        fi

        msg=":no_entry:  *Auto-retry inibido*\n\nTaxa de erro da rodagem ($err_pct%) está acima de 50% — provável incidente real.\nIntervenção humana necessária."

        payload=$(jq -n --arg ch "$SLACK_CHANNEL_ID" --arg t "$msg" --arg th "$thread_ts" '
            if $th != "" then {channel:$ch, text:$t, thread_ts:$th}
            else {channel:$ch, text:$t} end
        ')

        curl -s -X POST "https://slack.com/api/chat.postMessage" \
            -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
            -H "Content-Type: application/json" \
            -d "$payload" > /dev/null
    fi
    exit 0
fi

# ── 4. Filtra casos transientes ──
TRANSIENT_CATS='["DOC_GENERATION","LLM_TRANSIENT","ENRICHMENT_API","LLM_RATE_LIMIT","SHEETS"]'

failed_processes=$(jq -r --argjson cats "$TRANSIENT_CATS" '
    .summary.cases[]
    | select(.status == "error")
    | select(.error_category as $c | $cats | index($c))
    | .numero_processo
' "$SUMMARY_FILE")

if [ -z "$failed_processes" ]; then
    echo "[SKIP] Nenhum caso transiente para reprocessar."
    exit 0
fi

# Conta + monta lista CSV
n_failed=$(echo "$failed_processes" | wc -l | tr -d ' ')
processes_csv=$(echo "$failed_processes" | tr '\n' ',' | sed 's/,$//')

echo "[OK] $n_failed caso(s) transiente(s) identificado(s) para retry:"
echo "$failed_processes" | sed 's/^/    • /'
echo ""

# ── 5. Carrega .env ──
if [ -f "$ENV_FILE" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        line=$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
        [[ -z "$line" || "$line" == \#* ]] && continue
        export "$line"
    done < "$ENV_FILE"
fi

# ── 6. Posta header de retry no Slack ANTES de rodar ──
if [ -n "${SLACK_BOT_TOKEN:-}" ] && [ -n "${SLACK_CHANNEL_ID:-}" ]; then
    thread_ts=""
    if [ -f "$LOG_DIR/.slack_thread_ts" ]; then
        thread_ts=$(jq -r '.thread_ts // ""' "$LOG_DIR/.slack_thread_ts" 2>/dev/null || echo "")
    fi

    if [ -n "$thread_ts" ]; then
        retry_msg=":arrows_counterclockwise:  *Reprocessamento automático* — $n_failed caso(s) com falha transiente\n\n_Retry único agendado 30min após a rodagem principal._"

        payload=$(jq -n --arg ch "$SLACK_CHANNEL_ID" --arg t "$retry_msg" --arg th "$thread_ts" \
            '{channel:$ch, text:$t, thread_ts:$th}')

        curl -s -X POST "https://slack.com/api/chat.postMessage" \
            -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
            -H "Content-Type: application/json" \
            -d "$payload" > /dev/null
        echo "[SLACK] Header de retry postado na thread do dia."
    else
        echo "[SLACK] Sem thread_ts cacheado — pulando header. (Pipeline criará/reutilizará a thread.)"
    fi
fi

# ── 7. Ativa venv ──
if [ -f "$VENV" ]; then
    source "$VENV"
else
    echo "[ERRO] venv não encontrado em $VENV"
    exit 1
fi

# ── 8. Roda o pipeline com filtro + supressão de SLO ──
export PYTHONPATH="$LEXIA_DIR/src"
export LEXIA_TARGET_PROCESSES="$processes_csv"
# Bloco SLO já não é postado por default desde 29/04/2026 — não precisa
# setar LEXIA_SUPPRESS_SLO_BLOCK aqui.
export LEXIA_SPREADSHEET_ID="${LEXIA_SPREADSHEET_ID:?Defina LEXIA_SPREADSHEET_ID no .env}"

echo "[RUN] Disparando retry com $n_failed processo(s)..."
PYTHONUNBUFFERED=1 python3 -u "$LEXIA_DIR/scripts/run_traced_pipeline.py"
exit_code=$?

echo "========================================"
echo "[LexIA Retry] Finalizado — exit code: $exit_code — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

exit $exit_code
