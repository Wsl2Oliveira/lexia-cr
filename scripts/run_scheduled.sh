#!/usr/bin/env bash
# LexIA CR — Wrapper para execução agendada via launchd
# Valida certs mTLS, ativa venv, carrega .env e executa o pipeline.

set -euo pipefail

LEXIA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$LEXIA_DIR/logs"
VENV="$LEXIA_DIR/.venv/bin/activate"
ENV_FILE="$LEXIA_DIR/.env"

NU_CERT="$HOME/dev/nu/.nu/certificates/ist/prod/cert.pem"
NU_KEY="$HOME/dev/nu/.nu/certificates/ist/prod/key.pem"
NU_TOKEN="$HOME/dev/nu/.nu/tokens/br/prod/access"

mkdir -p "$LOG_DIR"

TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/pipeline-$TODAY.log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "========================================"
echo "[LexIA] Execução agendada — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

# ── Rotação de logs (manter últimos 30 dias) ──
find "$LOG_DIR" -name "pipeline-*.log" -mtime +30 -delete 2>/dev/null || true

# ── Validação de certificados mTLS ──
certs_ok=true
for f in "$NU_CERT" "$NU_KEY" "$NU_TOKEN"; do
    if [ ! -f "$f" ]; then
        echo "[ERRO] Arquivo não encontrado: $f"
        certs_ok=false
    fi
done

if [ "$certs_ok" = false ]; then
    echo "[ERRO] Certificados mTLS ausentes. Execute 'nucli' para renovar."

    # Notificar via Slack se possível
    source "$ENV_FILE" 2>/dev/null || true
    if [ -n "${SLACK_BOT_TOKEN:-}" ] && [ -n "${SLACK_CHANNEL_ID:-}" ]; then
        curl -s -X POST "https://slack.com/api/chat.postMessage" \
            -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
            -H "Content-Type: application/json" \
            -d "{
                \"channel\": \"$SLACK_CHANNEL_ID\",
                \"text\": \":rotating_light: *[LexIA - Alerta de Execução]*\n\nA execução agendada de $(date '+%d/%m/%Y %H:%M') *falhou* antes de iniciar.\n\n*Motivo:* Certificados mTLS do \`nucli\` não encontrados.\n*Ação:* Executar \`nucli\` no terminal para renovar os certificados e garantir que a máquina esteja conectada à VPN.\"
            }" > /dev/null
        echo "[SLACK] Alerta de certs ausentes enviado."
    fi

    exit 1
fi

# ── Validação de expiração do certificado ──
cert_expiry=$(openssl x509 -enddate -noout -in "$NU_CERT" 2>/dev/null | cut -d= -f2)
if [ -n "$cert_expiry" ]; then
    expiry_epoch=$(date -j -f "%b %d %H:%M:%S %Y %Z" "$cert_expiry" +%s 2>/dev/null || echo "0")
    now_epoch=$(date +%s)
    if [ "$expiry_epoch" -le "$now_epoch" ] 2>/dev/null; then
        echo "[ERRO] Certificado mTLS expirado em: $cert_expiry"

        source "$ENV_FILE" 2>/dev/null || true
        if [ -n "${SLACK_BOT_TOKEN:-}" ] && [ -n "${SLACK_CHANNEL_ID:-}" ]; then
            curl -s -X POST "https://slack.com/api/chat.postMessage" \
                -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
                -H "Content-Type: application/json" \
                -d "{
                    \"channel\": \"$SLACK_CHANNEL_ID\",
                    \"text\": \":rotating_light: *[LexIA - Alerta de Execução]*\n\nA execução agendada de $(date '+%d/%m/%Y %H:%M') *falhou* antes de iniciar.\n\n*Motivo:* Certificado mTLS expirado em $cert_expiry.\n*Ação:* Executar \`nucli\` no terminal para renovar os certificados.\"
                }" > /dev/null
            echo "[SLACK] Alerta de cert expirado enviado."
        fi

        exit 1
    fi
    echo "[OK] Cert válido até: $cert_expiry"
fi

# ── Carregar .env ──
if [ -f "$ENV_FILE" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        line=$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
        [[ -z "$line" || "$line" == \#* ]] && continue
        export "$line"
    done < "$ENV_FILE"
    echo "[OK] .env carregado"
else
    echo "[ERRO] Arquivo .env não encontrado em $ENV_FILE"
    exit 1
fi

# ── Ativar venv ──
if [ -f "$VENV" ]; then
    source "$VENV"
    echo "[OK] venv ativado ($(python3 --version))"
else
    echo "[ERRO] venv não encontrado em $VENV"
    exit 1
fi

# ── Executar pipeline ──
export PYTHONPATH="$LEXIA_DIR/src"
export LEXIA_SPREADSHEET_ID="${LEXIA_SPREADSHEET_ID:?Defina LEXIA_SPREADSHEET_ID no .env}"

echo "[RUN] Iniciando pipeline..."
python3 "$LEXIA_DIR/scripts/run_traced_pipeline.py"
exit_code=$?

echo "========================================"
echo "[LexIA] Finalizado — exit code: $exit_code — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

exit $exit_code
