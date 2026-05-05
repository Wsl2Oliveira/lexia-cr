#!/usr/bin/env bash
# LexIA CR — Wrapper para execução agendada via launchd
# Valida certs mTLS, ativa venv, carrega .env e executa o pipeline.
#
# PATH precisa ser hardcoded porque launchd/cron NÃO herdam o PATH do shell
# interativo do usuário. Sem isso:
#   - `nu` (binário do nucli) não é encontrado → auto-refresh do token falha
#   - GNU bash/awk de Homebrew não são encontrados → nucli quebra
#     (nucli exige Bash 4.0+ e GNU awk; macOS vem com Bash 3.2 + BSD awk)
#   - `python3` pode resolver pra Python do sistema (sem dependências do venv)
# Diretórios incluídos no PATH:
#   - $HOME/dev/nu/nucli  → binário `nu` (path canônico do nucli)
#   - /opt/homebrew/bin   → bash 4+, gawk, Python (Apple Silicon)
#   - /usr/local/bin      → bash 4+, gawk, Python (Intel Mac)
#   - /usr/bin:/bin       → utilitários base (date, openssl, etc.)
export PATH="$HOME/dev/nu/nucli:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"

# NU_HOME é mandatório: o nucli usa essa env var para resolver os paths de
# certs, tokens, VPN config, etc. (vide constants.sh do nucli). Sem ela,
# `nu auth get-access-token` falha com "variável não associada".
# Default canônico do nucli: $HOME/dev/nu. Se já estiver setada (ex.: chamada
# manual com env do shell), respeitamos.
export NU_HOME="${NU_HOME:-$HOME/dev/nu}"

set -euo pipefail

LEXIA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$LEXIA_DIR/logs"
VENV="$LEXIA_DIR/.venv/bin/activate"
ENV_FILE="$LEXIA_DIR/.env"
LOCK_FILE="$LOG_DIR/.pipeline.lock"

NU_CERT="$HOME/dev/nu/.nu/certificates/ist/prod/cert.pem"
NU_KEY="$HOME/dev/nu/.nu/certificates/ist/prod/key.pem"
NU_TOKEN="$HOME/dev/nu/.nu/tokens/br/prod/access"

mkdir -p "$LOG_DIR"

TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/pipeline-$TODAY.log"

exec > >(tee -a "$LOG_FILE") 2>&1

# ── Lock para evitar execuções concorrentes (rodagem principal vs retry) ──
if [ -f "$LOCK_FILE" ]; then
    lock_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
        echo "[LOCK] Outra execução em curso (PID $lock_pid). Abortando."
        exit 0
    fi
    echo "[LOCK] Lock órfão encontrado (PID $lock_pid não roda mais). Removendo."
    rm -f "$LOCK_FILE"
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT INT TERM

echo "========================================"
echo "[LexIA] Execução agendada — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

# ── Rotação de logs (manter últimos 30 dias) ──
find "$LOG_DIR" -name "pipeline-*.log" -mtime +30 -delete 2>/dev/null || true

# ── Auto-refresh silencioso do token Bearer (sem 2FA) ──
# `nu auth get-access-token` renova o access token via refresh-token salvo
# em disco, sem prompt 2FA. Roda a cada execução para evitar que o pipeline
# falhe na próxima vez que o token de 7d expirar. A intervenção manual
# (com 2FA via `nucli`) só é necessária quando o refresh-token de ~30d
# também expira — tipicamente 1x por mês.
# O Python (`run_traced_pipeline.py::ensure_nu_auth`) faz a mesma chamada
# como defense-in-depth — aqui no shell é proativo, lá é defensivo.
if command -v nu >/dev/null 2>&1; then
    if nu auth get-access-token >/dev/null 2>&1; then
        echo "[OK] nucli auth refresh silencioso (sem 2FA)"
    else
        echo "[AVISO] nucli auth refresh falhou — pode precisar de 2FA manual"
    fi
else
    echo "[AVISO] binário 'nu' não encontrado no PATH — pulando auto-refresh"
fi

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
# Usamos `openssl x509 -checkend` (forma canônica e cross-platform) em vez de
# parsear data com `date -j -f`, que era frágil em ambientes sem LANG/locale
# en_US (ex.: launchd) e gerava falso positivo de "expirado".
# Exit 0 do checkend = NÃO vai expirar nos próximos N segundos. Exit 1 = vai.
if ! openssl x509 -checkend 0 -noout -in "$NU_CERT" >/dev/null 2>&1; then
    cert_expiry=$(openssl x509 -enddate -noout -in "$NU_CERT" 2>/dev/null | cut -d= -f2)
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
cert_expiry=$(openssl x509 -enddate -noout -in "$NU_CERT" 2>/dev/null | cut -d= -f2)
echo "[OK] Cert válido até: $cert_expiry"

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
PYTHONUNBUFFERED=1 python3 -u "$LEXIA_DIR/scripts/run_traced_pipeline.py"
exit_code=$?

echo "========================================"
echo "[LexIA] Finalizado — exit code: $exit_code — $(date '+%d/%m/%Y %H:%M:%S')"
echo "========================================"

exit $exit_code
