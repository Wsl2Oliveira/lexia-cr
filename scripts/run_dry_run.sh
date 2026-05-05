#!/usr/bin/env bash
# LexIA CR — Simulação (DRY-RUN)
#
# Roda o pipeline real em modo simulação:
#   - Slack:  desligado (SLACK_NOTIFY_ENABLED=false)
#   - Drive:  generate_doc retorna URL fake (LEXIA_DRY_RUN=true)
#   - Sheets: append_rows é pulado     (LEXIA_DRY_RUN=true)
#   - Dedup:  desligada (processa todos os casos do range, mesmo já feitos)
#
# Uso: bash scripts/run_dry_run.sh [--days N]
#   --days N  override do DAYS_BACK só nesta execução (default: usa .env)
#
# Saída: tudo em STDOUT + tee para logs/dry-run-YYYY-MM-DD-HHMMSS.log
# Não impacta produção: nada é persistido em sistemas externos.

set -euo pipefail

LEXIA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$LEXIA_DIR/logs"
VENV="$LEXIA_DIR/.venv/bin/activate"
ENV_FILE="$LEXIA_DIR/.env"

NU_CERT="$HOME/dev/nu/.nu/certificates/ist/prod/cert.pem"

mkdir -p "$LOG_DIR"

TS=$(date '+%Y-%m-%d-%H%M%S')
LOG_FILE="$LOG_DIR/dry-run-$TS.log"

# Captura --days opcional
DAYS_OVERRIDE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --days)
            DAYS_OVERRIDE="$2"
            shift 2
            ;;
        *)
            echo "[ERRO] Argumento desconhecido: $1"
            exit 1
            ;;
    esac
done

exec > >(tee -a "$LOG_FILE") 2>&1

echo "============================================================"
echo "[DRY-RUN] LexIA — Simulação iniciada $(date '+%d/%m/%Y %H:%M:%S')"
echo "[DRY-RUN] Log: $LOG_FILE"
echo "============================================================"

# ── Validação de cert mTLS (só warning, não bloqueia) ──
if [ ! -f "$NU_CERT" ]; then
    echo "[AVISO] Cert mTLS não encontrado em $NU_CERT — APIs Nu vão falhar"
    echo "[AVISO] Continuando mesmo assim (dry-run)..."
fi

# ── Carregar .env ──
if [ ! -f "$ENV_FILE" ]; then
    echo "[ERRO] .env não encontrado em $ENV_FILE"
    exit 1
fi

while IFS= read -r line || [ -n "$line" ]; do
    line=$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
    [[ -z "$line" || "$line" == \#* ]] && continue
    export "$line"
done < "$ENV_FILE"

# ── Overrides DRY-RUN (sobrescrevem o .env) ──
export LEXIA_DRY_RUN=true
export SLACK_NOTIFY_ENABLED=false
if [ -n "$DAYS_OVERRIDE" ]; then
    export DAYS_BACK="$DAYS_OVERRIDE"
    echo "[DRY-RUN] DAYS_BACK override: $DAYS_OVERRIDE"
fi

echo "[DRY-RUN] Modo decisão: ${LEXIA_DECISION_MODE:-llm}"
echo "[DRY-RUN] DAYS_BACK: ${DAYS_BACK:-3}"
echo "[DRY-RUN] Slack: OFF | Drive: OFF | Sheets: OFF | Dedup: OFF"
echo ""

# ── Ativar venv ──
if [ -f "$VENV" ]; then
    source "$VENV"
else
    echo "[ERRO] venv não encontrado em $VENV"
    exit 1
fi

# ── Executar pipeline em foreground ──
export PYTHONPATH="$LEXIA_DIR/src"
PYTHONUNBUFFERED=1 python3 -u "$LEXIA_DIR/scripts/run_traced_pipeline.py"
exit_code=$?

echo ""
echo "============================================================"
echo "[DRY-RUN] Finalizado — exit code: $exit_code — $(date '+%d/%m/%Y %H:%M:%S')"
echo "[DRY-RUN] Log salvo em: $LOG_FILE"
echo "============================================================"

exit $exit_code
