"""Run the LexIA pipeline for specific cases with full traceability.

Queries Databricks for the target processes, enriches via Nubank APIs (if
certs are configured), gets the LLM macro decision, and writes every step
to a Google Sheets spreadsheet for QA review.

Usage:
    python scripts/run_traced_pipeline.py
"""

from __future__ import annotations

import json
import os
import secrets
import ssl
import string
import sys
import time
from datetime import UTC, datetime
from itertools import groupby
from pathlib import Path

import gspread
from gspread.utils import rowcol_to_a1
import httpx
from google.oauth2.service_account import Credentials
from openai import OpenAI
from slack_sdk import WebClient as SlackWebClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lexia.config import settings
from lexia.deterministic import decide as deterministic_decide
from lexia.monitoring import (
    ERROR_ACTIONS,
    CaseRecord,
    ErrorCategory,
    RunSummary,
    SLOReport,
    SLOTargets,
    categorize_case_error,
    compute_slo_report,
    write_run_summary_json,
)
from lexia.preflight import ensure_nu_auth

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TARGET_PROCESSES = settings.target_processes.split(",") if settings.target_processes else []

SPREADSHEET_ID = settings.spreadsheet_id
SHEET_NAME = "Relatorio_final"

IS_DRY_RUN = os.getenv("LEXIA_DRY_RUN", "").lower() in ("true", "1", "yes")
if IS_DRY_RUN:
    print("=" * 60)
    print("[DRY-RUN] Modo simulação ATIVO — Slack/Drive/Sheets não serão tocados.")
    print("=" * 60)

MESES_PT = [
    "",
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
]

HEADER_ROW = [
    "lexia_id",
    "id_oficio",
    "numero_processo",
    "tipo_oficio",
    "info_solicitada",
    "numero_oficio",
    "nome_investigado",
    "cpf_cnpj",
    "total_investigados",
    "vara_tribunal",
    "orgao_nome",
    "valor_solicitado",
    "is_cliente_nu",
    "data_recebimento",
    "waze_shard",
    "customers_customer_id",
    "nuconta_status",
    "nuconta_saldo",
    "mario_box_caixinhas",
    "crebito_cartoes",
    "rayquaza_saldo",
    "petrificus_bloqueios",
    "dados_bancarios",
    "llm_macro_aplicada",
    "llm_id_macro",
    "llm_texto_resposta",
    "llm_observacoes",
    "llm_raw_response",
    "doc_url",
    "status_execucao",
    "timestamp_execucao",
    # --- Responsiveness monitoring (JUD-1995) ---
    "started_at",
    "duration_secs",
    "error_category",
    # --- Deterministic engine bookkeeping (shadow/hybrid/deterministic modes) ---
    "det_id_macro",
    "det_macro_aplicada",
    "det_texto_resposta",
    "det_decision_source",
    "det_match_macro",
    "det_match_text_similarity",
    "det_confidence",
    "det_decision_reason",
]

TIPO_MAP = {
    "official_letter_type__block": "BLOQUEIO",
    "official_letter_type__dismiss": "DESBLOQUEIO",
    "official_letter_type__transfer": "TRANSFERÊNCIA",
}

_LEXIA_ID_CHARS = string.ascii_uppercase + string.digits


def generate_lexia_id() -> str:
    """Generate a unique execution ID like LX-7F3A-K9B2."""
    seg1 = "".join(secrets.choice(_LEXIA_ID_CHARS) for _ in range(4))
    seg2 = "".join(secrets.choice(_LEXIA_ID_CHARS) for _ in range(4))
    return f"LX-{seg1}-{seg2}"


def fetch_processed_oficios() -> set[str]:
    """Read the spreadsheet and return id_oficio values already completed."""
    sa_path = settings.google_service_account_path
    if not sa_path or not Path(sa_path).exists() or not SPREADSHEET_ID:
        return set()
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(SHEET_NAME)
        records = ws.get_all_values()
        if len(records) < 2:
            return set()
        header = records[0]
        oficio_idx = header.index("id_oficio") if "id_oficio" in header else 1
        status_idx = header.index("status_execucao") if "status_execucao" in header else -2
        return {
            row[oficio_idx]
            for row in records[1:]
            if len(row) > max(oficio_idx, status_idx) and row[status_idx] == "success"
        }
    except Exception as e:
        print(f"  [WARN] Não foi possível ler ofícios já concluídos: {e}")
        return set()


LLM_SYSTEM_PROMPT = """\
Você é um analista regulatório especializado em ordens judiciais da Nubank.
Analise os dados do caso judicial e decida qual macro de resposta aplicar.

MACROS DISPONÍVEIS (use o texto EXATO da coluna "Texto base", substituindo apenas as variáveis):

Macro 1 — DESBLOQUEIO REALIZADO (sem valores constritos)
Texto base: "os valores anteriormente atingidos por determinação judicial, encontram-se \
ativos e livres de qualquer bloqueio vinculado aos presentes autos."

Macro 1B — DESBLOQUEIO DE CONTA COM VALORES CONSTRITOS
Usar quando info_solicitada = "Desbloqueio De Conta" E existem bloqueios judiciais ativos \
(Petrificus status "frozen") cujos valores devem permanecer constritos.
Texto base: "a conta de pagamento encontra-se ativa e livre para movimentação, conforme \
determinado.\n\nCumpre esclarecer que permanecem constritos os valores de \
R$ [frozen_amount] ([valor por extenso]), bloqueados por força de determinação judicial \
prolatada anteriormente nos presentes autos, os quais não foram objeto da ordem de \
desbloqueio."

Macro 2 — NÃO POSSUI CONTA ATIVA
Texto base: "inexiste conta ativa em seu nome, pelo que resta inviabilizado o \
cumprimento da ordem."

Macro 3 — SALDO ZERADO OU VALOR ÍNFIMO
Texto base: "inexistem valores passíveis de bloqueio, pelo que resta inviabilizado o \
cumprimento da ordem."

Macro 4 — BLOQUEIO DE VALOR IGUAL AO DETERMINADO
Texto base: "foi bloqueado o importe disponível de R$ [variável de valor].\n\n\
Cumpre esclarecer que o valor bloqueado pode incluir ativos de baixa liquidez e/ou \
sujeitos a variações de mercado em razão da própria natureza do investimento no qual \
o importe está alocado. Por isso, caso a transferência desses valores seja determinada \
futuramente, o montante final poderá ser alterado, momento em que serão informadas \
as especificidades, caso existam."

Macro 5 — EXISTÊNCIA DE BLOQUEIO ANTERIOR. BLOQUEIO DE VALOR IGUAL AO DETERMINADO.
Texto base: "foi bloqueado o importe disponível de R$ [variável de valor] em benefício \
deste processo, existindo, ainda, outros valores bloqueados em razão de determinações \
judiciais prolatadas anteriormente.\n\nCumpre esclarecer que o valor bloqueado pode \
incluir ativos de baixa liquidez e/ou sujeitos a variações de mercado em razão da \
própria natureza do investimento no qual o importe está alocado. Por isso, caso a \
transferência desses valores seja determinada futuramente, o montante final poderá \
ser alterado, momento em que serão informadas as especificidades, caso existam."

Macro 6 — BLOQUEIO TOTAL DA CONTA DE PAGAMENTO
Texto base: "a conta de pagamento foi bloqueada, nesta data com saldo de \
R$ [variável de valor]."

Macro 7 — CLIENTE NÃO TEM CONTA DO NUBANK, SÓ TEM CARTÃO DE CRÉDITO
Texto base: "inexiste conta ativa em seu nome, pelo que resta inviabilizado o \
cumprimento da ordem."

Macro 8 — BLOQUEIO DE CARTÃO DE CRÉDITO
Texto base: "o cartão de crédito foi bloqueado, bem como nosso sistema foi parametrizado \
para a não liberação de novo cartão com a função crédito."

Macro 9 — NEGATIVA BLOQUEIO DE CARTÃO DE CRÉDITO
Texto base: "inexiste cartão de crédito nesta instituição na presente data. Informamos \
também que o nosso sistema está parametrizado para a não liberação de cartão com a \
função crédito."

--- MACROS DE TRANSFERÊNCIA ---

Macro T1 — CONTA ZERADA (TRANSFERÊNCIA INVIÁVEL)
Usar quando a conta do cliente existe mas o saldo é zero ou ínfimo, impossibilitando \
a transferência.
Texto base: "a conta do(a) cliente supra encontra-se zerada na data desta resposta. \
Em função do exposto, o atendimento à ordem judicial de transferência e depósito em juízo \
resta prejudicado por inexistência de saldo."

Macro T2 — NÃO É CLIENTE (TRANSFERÊNCIA)
Usar quando o CPF/CNPJ referido no ofício de transferência não consta no cadastro \
de clientes da Nubank.
Texto base: "o [CPF ou CNPJ] referido no ofício é o de número [documento], vimos \
informar por meio desta que esse [CPF ou CNPJ] não consta em nossos cadastros de clientes."

Macro T3 — TRANSFERÊNCIA REALIZADA
Usar quando o cliente possui saldo disponível e a transferência é viável. \
Os dados de transferência (ID, banco destino) devem ser preenchidos após execução.
Texto base: "em cumprimento à referida ordem judicial, comunicamos que o(a) cliente \
supra possui as seguintes posições na data desta resposta:\n\n\
- Saldo em conta: R$ [saldo_conta]\n\
- Fatura cartão de crédito (Valor a vencer + Valor vencido) R$ [valor_fatura]\n\
- [Empréstimos: R$ X ou Não há empréstimos]\n\
- [Não há seguro de vida / Não há investimentos / Não há criptoativos / ...]\n\n\
Ademais, oportuno esclarecer que as informações supra, referem-se à totalidade de \
produtos contratados pelo(a) cliente na data desta resposta."

REGRA FUNDAMENTAL — O TIPO DO OFÍCIO É DETERMINANTE:
- O campo "tipo_oficio" (BLOQUEIO, DESBLOQUEIO ou TRANSFERÊNCIA) define qual GRUPO de macros usar.
- Se tipo_oficio = BLOQUEIO → use APENAS macros de bloqueio (2, 3, 4, 5, 6, 7, 8, 9).
  NUNCA use Macro 1 (Desbloqueio Realizado) para um ofício de BLOQUEIO.
- Se tipo_oficio = DESBLOQUEIO → use APENAS Macro 1 (Desbloqueio Realizado).
  NUNCA use macros de bloqueio para um ofício de DESBLOQUEIO.
- Se tipo_oficio = TRANSFERÊNCIA → use APENAS macros de transferência (T1, T2, T3).
  NUNCA use macros de bloqueio ou desbloqueio para um ofício de TRANSFERÊNCIA.

REGRA DE ESPECIFICIDADE — INFO_SOLICITADA:
O campo "info_solicitada" (array) detalha o que a autoridade judicial ESPECIFICAMENTE
solicitou. Exemplos: "Bloqueio De Valores", "Bloqueio De Cartão", "Bloqueio De Conta",
"Desbloqueio De Conta", "Desbloqueio De Valores".
- Se contém "Bloqueio De Cartão" → priorize Macro 8 (bloqueio cartão) ou Macro 9
  (se não há cartão).
- Se contém "Bloqueio De Valores" → priorize Macros 3/4/5 conforme saldo.
- Se contém "Bloqueio De Conta" → priorize Macro 6 (bloqueio total da conta).
- Se contém múltiplos itens (ex: "Bloqueio De Valores" + "Bloqueio De Cartão"),
  combine as ações no texto e nas observações. Use a macro de maior impacto.
- O campo pode estar vazio ou null; nesse caso, use a regra geral pelo tipo_oficio.

REGRA DE SALDO COMPLETO — CAIXINHAS:
O campo "caixinhas" mostra o saldo em Money Boxes (caixinhas). O saldo total
bloqueável do cliente é: nuconta.saldo_disponivel + caixinhas.saldo_total.
- Para decidir se o saldo é "ínfimo", use o SALDO COMBINADO (NuConta + Caixinhas).
- Ao informar o valor bloqueado (Macro 4/5), inclua o saldo de caixinhas quando > 0.
  Exemplo: "foi bloqueado o importe disponível de R$ [saldo_nuconta + saldo_caixinhas]".

REGRAS DE CONTEXTO:
- DIFERENCIE bloqueio JUDICIAL (Petrificus/freeze-orders) de bloqueio COMERCIAL.
  - Petrificus status "dismissed" = bloqueio judicial anterior FOI encerrado.
  - Cartão "blocked" com "late_blocked" = bloqueio COMERCIAL (inadimplência), NÃO judicial.
  - Conta "internal_delinquent" = inadimplência, NÃO é bloqueio judicial.

REGRAS PARA DESBLOQUEIO (tipo_oficio = DESBLOQUEIO):
  - Se info_solicitada contém "Desbloqueio De Conta" (não "Desbloqueio De Valores") E
    existem bloqueios judiciais ativos (Petrificus status "frozen") → Macro 1B.
    O desbloqueio é da CONTA (livre para movimentação), mas os VALORES permanecem
    constritos. Use frozen_amount do Petrificus para informar o valor.
  - Se todos os bloqueios judiciais (Petrificus) estão "dismissed" ou inexistem → Macro 1.
  - Se a conta está bloqueada comercialmente (inadimplência), use Macro 1 mas ADICIONE
    ao final: "Informamos que eventuais restrições nos produtos do cliente decorrem de
    questões comerciais internas, não relacionadas à ordem judicial."

REGRA DE DADOS BANCÁRIOS:
Se o campo "info_solicitada" contém "Dados Bancarios" ou "Dados Bancários", você DEVE
incluir NO FINAL do "texto_resposta" (antes do fechamento da carta) um parágrafo adicional:
  "Informamos os dados da conta de pagamento: Banco Nu Pagamentos S.A. (260),
  Agência [agencia], Conta [conta]."
Use os dados do campo "dados_bancarios" (agencia, conta, banco).
Se dados_bancarios estiver vazio ou indisponível, omita este parágrafo.
IMPORTANTE: Inclua este texto DENTRO do texto_resposta, NÃO apenas em observacoes.

REGRAS PARA TRANSFERÊNCIA (tipo_oficio = TRANSFERÊNCIA):
  - Se o CPF/CNPJ NÃO é cliente (customer_id = NAO_CLIENTE) → Macro T2.
    Adapte "CPF ou CNPJ" conforme o tipo de documento.
  - Se é cliente MAS saldo combinado = 0 ou ínfimo (< R$ 10,00) → Macro T1.
    Informe apenas que a conta está zerada e a transferência é inviável.
  - Se é cliente E possui saldo significativo (>= R$ 10,00) → Macro T3.
    Detalhe a posição financeira completa: saldo NuConta, fatura cartão,
    empréstimos, investimentos, caixinhas. Inclua "Não há [produto]" quando
    o cliente não possui aquele produto.
  - Para Macro T3, se houver dados bancários, inclua agência e conta ao final.

REGRA OBRIGATÓRIA — ANÁLISE PRÉ-CALCULADA:
  O campo "analise_pre_calculada.macro_sugerida" contém a macro determinada pelo sistema
  com base em regras numéricas precisas. Se este campo NÃO for null, você DEVE usar
  a macro indicada. NÃO ignore esta indicação.

REGRAS PARA BLOQUEIO (tipo_oficio = BLOQUEIO):
  Use o SALDO COMBINADO (saldo_combinado.total = NuConta + Caixinhas) como referência:
  - Sem NuConta e sem conta crédito → Macro 2 (inexiste conta ativa).
  - Se saldo_combinado.is_infimo = true → Macro 3 (saldo zerado/ínfimo).
    Valores residuais não são passíveis de bloqueio efetivo.
  - Com NuConta, saldo combinado >= R$ 10,00 e SEM bloqueio judicial anterior → Macro 4.
  - Com NuConta, saldo combinado >= R$ 10,00 e COM bloqueios judiciais anteriores → Macro 5.
  - Bloqueio total da conta de pagamento → Macro 6 (valor = saldo).
  - Sem NuConta, sem conta crédito, mas COM cartão de crédito → Macro 7.
  - Cartão de crédito que precisa ser bloqueado → Macro 8.
  - Sem cartão de crédito → Macro 9.
- Substitua [variável de valor] pelo saldo_combinado.total (NuConta + Caixinhas).

REGRAS DE REDAÇÃO:
- Use o texto base da macro correspondente como resposta. NÃO invente textos diferentes.
- Substitua apenas as variáveis marcadas com [variável de valor].
- O texto_resposta complementa a frase "...informamos que" — já começa com letra minúscula.
- NÃO repita "Em atenção ao ofício judicial" — isso já consta no template.
- Revise a ortografia: cidades com acentuação correta (SÃO PAULO, não SAO PAULO).

Formato de saída OBRIGATÓRIO (JSON):
{
    "macro_aplicada": "Nome da macro (ex: DESBLOQUEIO REALIZADO, CONTA ZERADA + MONITORAMENTO)",
    "id_macro": "1-9 para bloqueio/desbloqueio, T1/T2/T3 para transferência",
    "valor_bloqueio": "valor em R$ ou null",
    "texto_resposta": "Texto base da macro com variáveis preenchidas",
    "observacoes": "Observações ou null"
}

Responda APENAS com o JSON.
"""

# ---------------------------------------------------------------------------
# Phase 1 — Databricks
# ---------------------------------------------------------------------------

QUERY_BASE = """
WITH ol_exploded AS (
    SELECT
        ol.official_letter__id,
        ol.official_letter__type,
        ol.official_letter__status,
        ol.official_letter__submission_id,
        inv_pos,
        inv_id,
        SIZE(ol.investigated_information__id) AS total_investigados
    FROM etl.br__dataset.jud_athena_official_letters ol
    LATERAL VIEW POSEXPLODE(ol.investigated_information__id) AS inv_pos, inv_id
),

base AS (
    SELECT
        ext.official_letter_extraction__created_at               AS data_recebimento,
        ole.official_letter__id                                   AS id_oficio,
        ole.official_letter__type                                 AS tipo_oficio,
        ole.official_letter__status                               AS status_oficio,
        ext.official_letter_extraction__craft_document_number      AS numero_oficio,
        ext.official_letter_extraction__process_document_number    AS numero_processo,
        ext.official_letter_extraction__court_tribunal_name        AS vara_tribunal,
        ext.official_letter_extraction__organ_name                 AS orgao_nome,
        ole.inv_id                                                 AS investigated_id,
        ole.inv_pos                                                AS investigado_seq,
        ole.total_investigados,
        name_pii.investigated_information__name                    AS nome_investigado,
        cpf_pii.investigated_information__cpf_cnpj                 AS cpf_cnpj,
        inv.investigated_information__requested_value              AS valor_solicitado,
        inv.investigated_information__customer_id                  AS customer_id,
        inv.investigated_information__is_customer                  AS is_cliente_nu,
        ext.official_letter_extraction__is_reiteration             AS is_reiteracao,
        ext.official_letter_extraction__confirmed_or_rejected_at   AS triado_em,
        ext.official_letter_extraction__confirmed_or_rejected_by   AS triado_por,
        ext.official_letter_extraction__requested_information       AS info_solicitada

    FROM etl.br__dataset.jud_athena_official_letter_extractions ext

    INNER JOIN etl.br__dataset.jud_athena_submissions sub
        ON ext.official_letter_extraction__submission_id = sub.submission__id

    INNER JOIN ol_exploded ole
        ON ole.official_letter__submission_id = sub.submission__id

    LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
        ON inv.investigated_information__id = ole.inv_id

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_name_pii name_pii
        ON name_pii.hash = inv.investigated_information__name

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_cpf_cnpj_pii cpf_pii
        ON cpf_pii.hash = inv.investigated_information__cpf_cnpj

    WHERE ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
      AND ole.official_letter__type IN (
          'official_letter_type__block',
          'official_letter_type__dismiss',
          'official_letter_type__transfer'
      )
      AND ext.official_letter_extraction__process_document_number IS NOT NULL
      {extra_filter}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_oficio, investigated_id
            ORDER BY triado_em DESC
        ) AS rn
    FROM base
)

SELECT *
FROM ranked
WHERE rn = 1
ORDER BY data_recebimento DESC, numero_processo, investigado_seq
"""


def fetch_cases_from_databricks(
    processes: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Query Databricks for cases.

    When *processes* is given, filters by those process numbers.
    Otherwise, fetches the latest deduped cases (optionally limited to *limit*).
    """
    from databricks import sql as dbsql

    if processes:
        placeholders = ", ".join(f"'{p}'" for p in processes)
        extra = f"AND ext.official_letter_extraction__process_document_number IN ({placeholders})"
    else:
        days = int(os.environ.get("DAYS_BACK", "3"))
        extra = (
            f"AND ext.official_letter_extraction__created_at"
            f" >= current_date() - INTERVAL {days} DAYS"
        )

    query = QUERY_BASE.format(extra_filter=extra)
    if limit:
        query = query.rstrip().rstrip(";")
        query += f"\nLIMIT {limit}"

    if processes:
        label = f"{len(processes)} processos"
    else:
        label = f"últimos casos (LIMIT {limit or 'ALL'})"
    print(f"\n[FASE 1] Databricks — consultando {label}...")
    print(f"  Host: {settings.databricks_host}")

    with dbsql.connect(
        server_hostname=settings.databricks_host.replace("https://", ""),
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

    cases = [dict(zip(columns, row, strict=False)) for row in rows]
    print(f"  ✓ {len(cases)} registros retornados (deduplicados)")
    for c in cases:
        tipo = TIPO_MAP.get(c.get("tipo_oficio", ""), c.get("tipo_oficio", ""))
        nome = c.get("nome_investigado", "N/A")
        print(f"    • {c.get('numero_processo')} | {tipo} | {nome}")

    if processes:
        not_found = set(processes) - {c.get("numero_processo") for c in cases}
        if not_found:
            print(f"  ⚠ Processos não encontrados: {not_found}")

    return cases


# ---------------------------------------------------------------------------
# Phase 2 — Nubank APIs (enrichment)
# ---------------------------------------------------------------------------

NU_CERT_PATH = Path.home() / "dev/nu/.nu/certificates/ist/prod/cert.pem"
NU_KEY_PATH = Path.home() / "dev/nu/.nu/certificates/ist/prod/key.pem"
NU_TOKEN_PATH = Path.home() / "dev/nu/.nu/tokens/br/prod/access"


def _has_nu_auth() -> bool:
    """Verifica se há tokens/certs em disco, com auto-refresh silencioso prévio.

    Antes de checar a existência dos arquivos, dispara `nu auth get-access-token`
    via ``ensure_nu_auth()``. Isso renova o access token (e o refresh token,
    quando aplicável) sem prompt 2FA, evitando que o pipeline falhe quando o
    operador não rodou ``nucli`` recentemente.

    A intervenção manual (com 2FA) só é necessária quando o refresh-token
    também expira — tipicamente uma vez por mês.
    """
    refresh_ok, refresh_reason = ensure_nu_auth()
    if not refresh_ok:
        print(f"[PREFLIGHT] auto-refresh do nucli falhou: {refresh_reason}")
    return NU_CERT_PATH.exists() and NU_KEY_PATH.exists() and NU_TOKEN_PATH.exists()


def _get_nu_client() -> httpx.Client:
    """Build an httpx client with nucli certs + cached bearer token.

    Garante token válido via ``ensure_nu_auth()`` antes de ler do disco.
    Se o refresh-token também tiver expirado, levanta RuntimeError com ação
    clara para o operador (rodar ``nucli`` com 2FA).
    """
    refresh_ok, refresh_reason = ensure_nu_auth()
    if not refresh_ok:
        raise RuntimeError(
            "[NU CLIENT] Não foi possível garantir token válido via "
            f"`nu auth get-access-token`: {refresh_reason}. "
            "Refresh-token provavelmente expirou. Ação: rodar `nucli` no "
            "terminal (com 2FA) para regenerar tokens."
        )

    token = NU_TOKEN_PATH.read_text().strip()
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.load_cert_chain(str(NU_CERT_PATH), str(NU_KEY_PATH))
    return httpx.Client(
        verify=ssl_ctx,
        timeout=30,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )


# Status codes que justificam retry: 5xx (instabilidade do servidor) e 429
# (rate limit). 4xx restantes são respostas válidas do servidor (CPF inválido,
# token expirado, etc.) e não devem ser retentados.
_NU_RETRY_STATUS = {429, 500, 502, 503, 504}

# Exceções de transporte que indicam falha transitória de rede.
_NU_RETRY_EXC = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.RemoteProtocolError,
    httpx.PoolTimeout,
)


def _nu_request(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    json_body: dict | None = None,
    params: dict | None = None,
    retries: int = 2,
    backoff: tuple[float, ...] = (1.0, 3.0),
    label: str = "",
) -> httpx.Response:
    """Wrap a Nubank internal API call with bounded retry on transient failures.

    Retries up to ``retries`` extra attempts (default: 1 + 2 = 3 total) on:
      - Transport errors (timeout, connection drop, protocol error).
      - HTTP 429 (rate limit) and 5xx (server-side errors).

    Does NOT retry on 4xx (except 429): those are deterministic server answers
    (404 = not customer, 400 = bad payload, 401/403 = auth) where retrying
    would only burn time.

    The caller still receives a normal :class:`httpx.Response` and is expected
    to handle business statuses (e.g. ``status_code == 404``) explicitly.
    """
    last_exc: Exception | None = None
    last_resp: httpx.Response | None = None
    total_attempts = retries + 1
    tag = f"[NU{(' ' + label) if label else ''}]"

    for attempt in range(1, total_attempts + 1):
        try:
            resp = client.request(method, url, json=json_body, params=params)
        except _NU_RETRY_EXC as exc:
            last_exc = exc
            if attempt < total_attempts:
                wait = backoff[min(attempt - 1, len(backoff) - 1)]
                print(
                    f"      {tag} {type(exc).__name__} em '{url}' "
                    f"(tentativa {attempt}/{total_attempts}), retentando em {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            raise

        if resp.status_code in _NU_RETRY_STATUS and attempt < total_attempts:
            last_resp = resp
            wait = backoff[min(attempt - 1, len(backoff) - 1)]
            if resp.status_code == 429:
                wait = max(wait, 5.0)
            print(
                f"      {tag} HTTP {resp.status_code} em '{url}' "
                f"(tentativa {attempt}/{total_attempts}), retentando em {wait:.1f}s..."
            )
            time.sleep(wait)
            continue

        return resp

    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"_nu_request: estado inesperado para {url}")


def enrich_case(case: dict) -> dict:
    """Call Nubank internal APIs for a single case and return trace logs."""
    trace = {
        "waze_shard": "N/A",
        "customers_customer_id": "N/A",
        "nuconta_status": "N/A",
        "nuconta_saldo": "N/A",
        "crebito_cartoes": "N/A",
        "rayquaza_saldo": "N/A",
        "petrificus_bloqueios": "N/A",
        "mario_box_caixinhas": "N/A",
        "dados_bancarios": "N/A",
        "savings_account": {},
        "cards": [],
        "assets": [],
        "blocks": [],
    }

    tax_id = case.get("cpf_cnpj")
    if not tax_id:
        trace["waze_shard"] = "SEM_CPF"
        return trace

    clean_id = tax_id.strip().replace(".", "").replace("-", "").replace("/", "")
    is_pj = len(clean_id) > 11

    if not _has_nu_auth():
        for k in [
            "waze_shard",
            "customers_customer_id",
            "nuconta_status",
            "nuconta_saldo",
            "crebito_cartoes",
            "rayquaza_saldo",
            "petrificus_bloqueios",
            "dados_bancarios",
        ]:
            trace[k] = "CERTS_NAO_CONFIGURADOS"
        return trace

    try:
        client = _get_nu_client()

        # Waze — endpoint diferente para PJ (CNPJ) vs PF (CPF)
        if is_pj:
            waze_url = "https://prod-global-waze.nubank.com.br/api/mapping/cnpj"
            waze_payload = {"cnpj": clean_id}
        else:
            waze_url = "https://prod-global-waze.nubank.com.br/api/mapping/cpf"
            waze_payload = {"cpf": clean_id}

        waze_resp = _nu_request(
            client, "POST", waze_url, json_body=waze_payload, label="Waze"
        )
        waze_resp.raise_for_status()
        shard = waze_resp.json().get("shard")
        trace["waze_shard"] = shard or "NOT_FOUND"

        if not shard:
            client.close()
            return trace

        # Customers — /company/ para PJ, /person/ para PF
        if is_pj:
            cust_url = (
                f"https://prod-{shard}-customers.nubank.com.br/api/customers/company/find-by-tax-id"
            )
        else:
            cust_url = (
                f"https://prod-{shard}-customers.nubank.com.br/api/customers/person/find-by-tax-id"
            )

        cust_resp = _nu_request(
            client,
            "POST",
            cust_url,
            json_body={"tax_id": clean_id},
            label="Customers",
        )
        if cust_resp.status_code == 404:
            trace["customers_customer_id"] = "NAO_CLIENTE"
            client.close()
            return trace
        cust_resp.raise_for_status()
        cust_data = cust_resp.json()
        inner = cust_data.get("customer", cust_data)
        customer_id = inner.get("id") or inner.get("customer_id")
        trace["customers_customer_id"] = customer_id or "NOT_FOUND"

        if not customer_id:
            client.close()
            return trace

        # savings-accounts — verifica se tem NuConta
        savings_account_id = None
        try:
            sa_resp = _nu_request(
                client,
                "GET",
                f"https://prod-{shard}-savings-accounts.nubank.com.br/api/customer/{customer_id}/savings-account",
                label="Savings",
            )
            if sa_resp.status_code == 404:
                trace["nuconta_status"] = "SEM_NUCONTA"
            else:
                sa_resp.raise_for_status()
                sa_data = sa_resp.json()
                sa_inner = sa_data.get("savings_account", sa_data)
                savings_account_id = sa_inner.get("id")
                trace["savings_account"] = sa_inner
                trace["nuconta_status"] = json.dumps(
                    {
                        "id": savings_account_id,
                        "status": sa_inner.get("status"),
                    },
                    ensure_ascii=False,
                )
        except Exception as e:
            trace["nuconta_status"] = f"ERRO: {e}"

        # Diablo — saldo NuConta (se tem savings-account)
        if savings_account_id:
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                diablo_resp = _nu_request(
                    client,
                    "GET",
                    f"https://prod-{shard}-diablo.nubank.com.br/api/savings-accounts/{savings_account_id}/balance/{today}",
                    label="Diablo",
                )
                if diablo_resp.status_code == 404:
                    trace["nuconta_saldo"] = "SEM_SALDO"
                else:
                    diablo_resp.raise_for_status()
                    diablo_data = diablo_resp.json()
                    bal = diablo_data.get("balance", diablo_data)
                    trace["nuconta_saldo"] = json.dumps(
                        {
                            "available": bal.get("available"),
                            "blocked": bal.get("blocked"),
                            "total": bal.get("total"),
                        },
                        ensure_ascii=False,
                    )
            except Exception as e:
                trace["nuconta_saldo"] = f"ERRO: {e}"
        else:
            trace["nuconta_saldo"] = "SEM_NUCONTA"

        # Facade (cartões + conta crédito) — mais completo que Crebito
        try:
            facade_resp = _nu_request(
                client,
                "GET",
                f"https://prod-{shard}-facade.nubank.com.br/api/customers/{customer_id}/account",
                label="Facade",
            )
            if facade_resp.status_code == 404:
                trace["crebito_cartoes"] = "SEM_CONTA_CREDITO"
            else:
                facade_resp.raise_for_status()
                facade_data = facade_resp.json()
                acct = facade_data.get("account", facade_data)
                acct_status = acct.get("status", "unknown")
                cards_list = acct.get("cards", [])
                trace["cards"] = cards_list
                balances = acct.get("balances", {})
                trace["crebito_cartoes"] = json.dumps(
                    {
                        "account_status": acct_status,
                        "cards": [
                            {
                                "status": c.get("status"),
                                "status_detail": c.get("status_detail"),
                                "last4": (c.get("card_number") or "")[-4:],
                            }
                            for c in cards_list
                        ],
                        "balances": {
                            "available": balances.get("available"),
                            "due": balances.get("due"),
                            "open": balances.get("open"),
                        },
                    },
                    ensure_ascii=False,
                )
        except Exception as e:
            trace["crebito_cartoes"] = f"ERRO: {e}"

        # Rayquaza — available-assets (includes NuConta + caixinhas/liquid_deposit)
        try:
            assets_resp = _nu_request(
                client,
                "GET",
                f"https://prod-{shard}-rayquaza.nubank.com.br/api/customers/{customer_id}/available-assets",
                label="Rayquaza",
            )
            if assets_resp.status_code == 404:
                trace["rayquaza_saldo"] = "SEM_ATIVOS"
            else:
                assets_resp.raise_for_status()
                assets_data = assets_resp.json()
                assets_list = (
                    assets_data if isinstance(assets_data, list) else assets_data.get("assets", [])
                )
                trace["assets"] = assets_list

                caixinhas_total = sum(
                    float(a.get("available_amount", 0) or 0)
                    for a in assets_list
                    if a.get("kind") == "liquid_deposit"
                )
                total_seizable = sum(float(a.get("available_amount", 0) or 0) for a in assets_list)
                trace["rayquaza_saldo"] = json.dumps(
                    {
                        "ativos": [
                            {
                                "kind": a.get("kind"),
                                "institution": a.get("institution"),
                                "available_amount": a.get("available_amount"),
                                "seizable": "seizable" in (a.get("categories") or []),
                            }
                            for a in assets_list
                        ],
                        "caixinhas_total": f"{caixinhas_total:.2f}",
                        "total_disponivel": f"{total_seizable:.2f}",
                    },
                    ensure_ascii=False,
                )
        except Exception as e:
            trace["rayquaza_saldo"] = f"ERRO: {e}"

        # Petrificus
        try:
            blocks_resp = _nu_request(
                client,
                "GET",
                f"https://prod-{shard}-petrificus-parcialus.nubank.com.br/api/customers/{customer_id}/freeze-orders",
                label="Petrificus",
            )
            if blocks_resp.status_code == 404:
                trace["petrificus_bloqueios"] = "SEM_BLOQUEIOS"
            else:
                blocks_resp.raise_for_status()
                blocks_data = blocks_resp.json()
                blocks_list = (
                    blocks_data
                    if isinstance(blocks_data, list)
                    else blocks_data.get("freeze_orders", [])
                )
                trace["blocks"] = blocks_list
                trace["petrificus_bloqueios"] = json.dumps(
                    [{"status": b.get("status"), "amount": b.get("amount")} for b in blocks_list],
                    ensure_ascii=False,
                )
        except Exception as e:
            trace["petrificus_bloqueios"] = f"ERRO: {e}"

        # Mario-Box — Caixinhas metadata (nomes, sem saldo — saldo vem do Rayquaza)
        try:
            mb_resp = _nu_request(
                client,
                "GET",
                f"https://prod-{shard}-mario-box.nubank.com.br/api/customers/{customer_id}/money-boxes",
                label="MarioBox",
            )
            if mb_resp.status_code == 404:
                trace["mario_box_caixinhas"] = "SEM_CAIXINHAS"
            else:
                mb_resp.raise_for_status()
                mb_data = mb_resp.json()
                boxes = (
                    mb_data
                    if isinstance(mb_data, list)
                    else mb_data.get("money_boxes", mb_data.get("moneyBoxes", []))
                )
                if isinstance(boxes, list):
                    trace["mario_box_caixinhas"] = json.dumps(
                        {
                            "quantidade": len(boxes),
                            "nomes": [b.get("name", "N/A") for b in boxes],
                        },
                        ensure_ascii=False,
                    )
                else:
                    trace["mario_box_caixinhas"] = json.dumps(mb_data, ensure_ascii=False)[:500]
        except Exception as e:
            trace["mario_box_caixinhas"] = f"ERRO: {e}"

        # Bank-accounts-widget-provider — agência e conta (se tem savings-account)
        if savings_account_id:
            try:
                ba_resp = _nu_request(
                    client,
                    "GET",
                    "https://prod-global-bank-accounts-widget-provider.nubank.com.br"
                    "/api/savings-accounts/resources/country-data",
                    params={
                        "customer-id": customer_id,
                        "savings-account-id": savings_account_id,
                    },
                    label="BankAccounts",
                )
                if ba_resp.status_code == 200:
                    ba_data = ba_resp.json()
                    acct_num = ba_data.get("account_number", "")
                    acct_dig = ba_data.get("account_number_digit", "")
                    trace["dados_bancarios"] = json.dumps(
                        {
                            "agencia": "0001",
                            "conta": f"{acct_num}-{acct_dig}" if acct_num else "N/A",
                            "banco": "Nu Pagamentos S.A. (260)",
                        },
                        ensure_ascii=False,
                    )
                else:
                    trace["dados_bancarios"] = "INDISPONIVEL"
            except Exception as e:
                trace["dados_bancarios"] = f"ERRO: {e}"
        else:
            trace["dados_bancarios"] = "SEM_NUCONTA"

        client.close()

    except Exception as e:
        for k in [
            "waze_shard",
            "customers_customer_id",
            "crebito_cartoes",
            "rayquaza_saldo",
            "petrificus_bloqueios",
            "dados_bancarios",
        ]:
            if trace[k] == "N/A":
                trace[k] = f"ERRO: {e}"

    return trace


# ---------------------------------------------------------------------------
# Phase 3 — LLM decision
# ---------------------------------------------------------------------------


def get_llm_decision(case: dict, enrichment: dict) -> dict:
    """Call LiteLLM for the macro decision and return trace data."""
    trace = {
        "llm_macro_aplicada": "N/A",
        "llm_id_macro": "N/A",
        "llm_texto_resposta": "N/A",
        "llm_observacoes": "N/A",
        "llm_raw_response": "N/A",
    }

    # Parse enrichment data for LLM context
    facade_info = {}
    try:
        facade_info = json.loads(enrichment.get("crebito_cartoes", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    nuconta_info = {}
    try:
        nuconta_info = json.loads(enrichment.get("nuconta_status", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    nuconta_saldo = {}
    try:
        nuconta_saldo = json.loads(enrichment.get("nuconta_saldo", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    threshold_infimo = 10.0
    saldo_disponivel_raw = nuconta_saldo.get("available", "0")
    try:
        saldo_float = float(saldo_disponivel_raw) if saldo_disponivel_raw else 0.0
    except (ValueError, TypeError):
        saldo_float = 0.0

    tipo_oficio = TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", ""))
    has_nuconta = nuconta_info.get("status") not in (None, "N/A", "not_found")
    saldo_infimo = saldo_float < threshold_infimo

    blocks_raw = enrichment.get("blocks", [])
    has_active_judicial_blocks = any(b.get("status") not in ("dismissed", None) for b in blocks_raw)
    has_credit_account = facade_info.get("account_status") not in (None, "N/A", "not_found")
    has_cards = bool(facade_info.get("cards"))

    # Compute frozen amount from active judicial blocks
    frozen_amount = sum(
        float(b.get("frozen_amount", b.get("amount", 0)) or 0)
        for b in blocks_raw
        if b.get("status") == "frozen"
    )

    macro_hint = None
    if tipo_oficio == "DESBLOQUEIO":
        macro_hint = "Macro 1 — desbloqueio"

    # Parse Rayquaza data for combined balance (NuConta + caixinhas/liquid_deposit)
    rayquaza_info = {}
    try:
        rayquaza_info = json.loads(enrichment.get("rayquaza_saldo", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    saldo_caixinhas = 0.0
    saldo_total_rayquaza = 0.0
    try:
        saldo_caixinhas = float(rayquaza_info.get("caixinhas_total", "0") or "0")
    except (ValueError, TypeError):
        pass
    try:
        saldo_total_rayquaza = float(rayquaza_info.get("total_disponivel", "0") or "0")
    except (ValueError, TypeError):
        pass

    saldo_combinado = max(saldo_total_rayquaza, saldo_float + saldo_caixinhas)
    saldo_combinado_infimo = saldo_combinado < threshold_infimo

    # Parse info_solicitada (requested_information from Athena)
    info_solicitada_raw = case.get("info_solicitada")
    if isinstance(info_solicitada_raw, str):
        try:
            info_solicitada = json.loads(info_solicitada_raw)
        except (json.JSONDecodeError, TypeError):
            info_solicitada = [info_solicitada_raw] if info_solicitada_raw else []
    elif isinstance(info_solicitada_raw, list):
        info_solicitada = info_solicitada_raw
    else:
        info_solicitada = []

    # Refine macro_hint for DESBLOQUEIO with active blocks
    if tipo_oficio == "DESBLOQUEIO":
        info_lower = [i.lower() for i in info_solicitada if i]
        is_desbloqueio_conta = any("desbloqueio" in i and "conta" in i for i in info_lower)
        is_desbloqueio_valores = any("desbloqueio" in i and "valor" in i for i in info_lower)

        if is_desbloqueio_conta and not is_desbloqueio_valores and frozen_amount > 0:
            macro_hint = (
                f"Macro 1B — desbloqueio de conta com valores constritos "
                f"(R$ {frozen_amount:.2f} permanecem bloqueados)"
            )

    # Transfer macro_hint
    if tipo_oficio == "TRANSFERÊNCIA":
        customer_id_val = enrichment.get("customers_customer_id", "N/A")
        if customer_id_val in ("NAO_CLIENTE", "NOT_FOUND"):
            macro_hint = "Macro T2 — não é cliente (transferência)"
        elif saldo_combinado_infimo:
            macro_hint = (
                f"Macro T1 — conta zerada, transferência inviável (saldo R$ {saldo_combinado:.2f})"
            )
        else:
            macro_hint = f"Macro T3 — transferência viável (saldo R$ {saldo_combinado:.2f})"

    # Refine macro_hint based on combined balance and info_solicitada
    if tipo_oficio == "BLOQUEIO":
        info_lower = [i.lower() for i in info_solicitada if i]
        is_cartao_request = any("cartão" in i or "cartao" in i for i in info_lower)
        is_valores_request = any("valores" in i for i in info_lower)
        is_conta_request = any("conta" in i and "bloqueio" in i for i in info_lower)

        if is_cartao_request and not is_valores_request:
            if has_cards:
                macro_hint = "Macro 8 — bloqueio de cartão de crédito"
            else:
                macro_hint = "Macro 9 — negativa bloqueio de cartão (não possui)"
        elif is_conta_request and not is_valores_request and not is_cartao_request:
            macro_hint = f"Macro 6 — bloqueio total da conta (saldo R$ {saldo_combinado:.2f})"
        else:
            if not has_nuconta and not has_credit_account:
                if has_cards:
                    macro_hint = "Macro 7 — sem conta, só cartão de crédito"
                else:
                    macro_hint = "Macro 2 — conta inexistente"
            elif saldo_combinado_infimo:
                macro_hint = (
                    f"Macro 3 — saldo ínfimo (R$ {saldo_combinado:.2f} < R$ {threshold_infimo:.2f})"
                )
            elif has_active_judicial_blocks:
                macro_hint = (
                    f"Macro 5 — bloqueio com bloqueios anteriores ativos"
                    f" (saldo R$ {saldo_combinado:.2f})"
                )
            else:
                macro_hint = f"Macro 4 — bloqueio padrão (saldo R$ {saldo_combinado:.2f})"

    # Parse dados bancários
    dados_bancarios = {}
    try:
        dados_bancarios = json.loads(enrichment.get("dados_bancarios", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    context = json.dumps(
        {
            "dados_caso": {
                "numero_processo": case.get("numero_processo"),
                "tipo_oficio": tipo_oficio,
                "numero_oficio": case.get("numero_oficio"),
                "nome_investigado": case.get("nome_investigado"),
                "cpf_cnpj": case.get("cpf_cnpj"),
                "vara_tribunal": case.get("vara_tribunal"),
                "orgao_nome": case.get("orgao_nome"),
                "valor_solicitado": str(case.get("valor_solicitado", "")),
                "is_cliente_nu": case.get("is_cliente_nu"),
                "customer_id": case.get("customer_id"),
                "info_solicitada": info_solicitada,
            },
            "nuconta": {
                "status": nuconta_info.get("status", "N/A"),
                "saldo_disponivel": nuconta_saldo.get("available", "N/A"),
                "saldo_bloqueado": nuconta_saldo.get("blocked", "N/A"),
                "saldo_total": nuconta_saldo.get("total", "N/A"),
                "saldo_is_infimo": saldo_infimo,
            },
            "caixinhas": {
                "saldo_total": f"{saldo_caixinhas:.2f}",
                "ativos_rayquaza": rayquaza_info.get("ativos", []),
            },
            "saldo_combinado": {
                "nuconta_disponivel": f"{saldo_float:.2f}",
                "caixinhas": f"{saldo_caixinhas:.2f}",
                "total": f"{saldo_combinado:.2f}",
                "is_infimo": saldo_combinado_infimo,
            },
            "analise_pre_calculada": {
                "macro_sugerida": macro_hint,
                "justificativa": macro_hint,
            },
            "conta_credito": {
                "account_status": facade_info.get("account_status", "N/A"),
                "cartoes": facade_info.get("cards", []),
                "saldos_credito": facade_info.get("balances", {}),
            },
            "ativos_disponiveis": enrichment.get("assets", []),
            "bloqueios_judiciais": enrichment.get("blocks", []),
            "frozen_amount_total": f"{frozen_amount:.2f}",
            "dados_bancarios": dados_bancarios,
        },
        ensure_ascii=False,
        indent=2,
    )

    user_message = f"Analise o caso abaixo e decida a macro:\n\n{context}"

    import time as _time

    # Throttle pré-chamada: suaviza pico para evitar rate limit (3 RPM observado)
    _time.sleep(8)

    # Fail-fast: 1 tentativa + 1 retry curto. Casos que falharem serão reprocessados manualmente.
    max_retries = 2
    backoff_secs = [5, 5]
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            client = OpenAI(
                api_key=settings.litellm_api_key,
                base_url=settings.litellm_base_url,
            )

            response = client.chat.completions.create(
                model=settings.litellm_model,
                messages=[
                    {
                        "role": "system",
                        "content": LLM_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {"role": "user", "content": user_message},
                ],
                temperature=0.2,
                max_tokens=1024,
                response_format={"type": "json_object"},
            )

            raw = response.choices[0].message.content.strip()
            trace["llm_raw_response"] = raw[:2000]

            cleaned = raw.removeprefix("```json").removesuffix("```").strip()
            parsed = json.loads(cleaned)

            trace["llm_macro_aplicada"] = parsed.get("macro_aplicada", "DESCONHECIDA")
            trace["llm_id_macro"] = str(parsed.get("id_macro", "0"))
            trace["llm_texto_resposta"] = parsed.get("texto_resposta", "")[:2000]
            trace["llm_observacoes"] = parsed.get("observacoes") or ""
            last_error = None
            break

        except json.JSONDecodeError:
            trace["llm_macro_aplicada"] = "ERRO_PARSE_JSON"
            trace["llm_observacoes"] = "LLM retornou resposta não-JSON"
            last_error = None
            break

        except Exception as e:
            last_error = e
            err_str = str(e)
            is_rate_limit = "429" in err_str or "RateLimit" in err_str or "rate_limit" in err_str
            if attempt < max_retries:
                wait = backoff_secs[attempt - 1]
                tag = "[LLM][429]" if is_rate_limit else "[LLM]"
                print(
                    f"    {tag} Tentativa {attempt}/{max_retries} falhou, retentando em {wait}s..."
                )
                _time.sleep(wait)

    if last_error is not None:
        trace["llm_macro_aplicada"] = "ERRO_LLM"
        trace["llm_observacoes"] = (
            f"Falhou após {max_retries} tentativas. Último erro: {last_error}"
        )[:500]

    return trace


# ---------------------------------------------------------------------------
# Decision dispatcher (LLM vs deterministic vs shadow vs hybrid)
# ---------------------------------------------------------------------------


def _text_similarity(a: str, b: str) -> float:
    """Return a 0..1 similarity ratio between two strings (difflib)."""
    from difflib import SequenceMatcher

    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _empty_det_trace(reason: str = "not_run") -> dict:
    return {
        "llm_macro_aplicada": "",
        "llm_id_macro": "",
        "llm_texto_resposta": "",
        "llm_observacoes": "",
        "llm_raw_response": f"[deterministic skipped: {reason}]",
        "decision_source": "skipped",
        "confidence": "N/A",
        "decision_reason": reason,
    }


def get_decision(case: dict, enrichment: dict) -> dict:
    """Dispatch between LLM and deterministic engine based on ``decision_mode``.

    Returns a trace dict in the same shape as ``get_llm_decision`` plus extra
    bookkeeping fields used by Sheets and the shadow-comparison report:

        - ``decision_source``: "llm" | "deterministic" | "llm_fallback"
        - ``confidence``:      "HIGH" | "LOW" | "N/A"
        - ``decision_reason``: short trace
        - ``det_*``:           per-mode shadow trace (when applicable)
    """
    mode = settings.decision_mode

    if mode == "llm":
        trace = get_llm_decision(case, enrichment)
        trace.setdefault("decision_source", "llm")
        trace.setdefault("confidence", "N/A")
        trace.setdefault("decision_reason", "LLM-only mode")
        trace["det_trace"] = _empty_det_trace("mode=llm")
        return trace

    if mode == "deterministic":
        det_trace = deterministic_decide(case, enrichment)
        if det_trace["confidence"] == "LOW" or det_trace["llm_macro_aplicada"].startswith("ERRO"):
            det_trace["llm_macro_aplicada"] = "ERRO_DETERMINISTIC_LOW_CONFIDENCE"
            det_trace["llm_observacoes"] = (
                f"Modo determinístico-only: {det_trace.get('decision_reason', '')}"
            )
        det_trace["det_trace"] = dict(det_trace)
        return det_trace

    if mode == "hybrid":
        det_trace = deterministic_decide(case, enrichment)
        if det_trace["confidence"] == "HIGH" and not det_trace["llm_macro_aplicada"].startswith("ERRO"):
            det_trace["det_trace"] = dict(det_trace)
            return det_trace

        print(
            f"    [HYBRID] confidence={det_trace['confidence']} → fallback p/ LLM "
            f"(motivo: {det_trace.get('decision_reason', '?')})"
        )
        llm_trace = get_llm_decision(case, enrichment)
        llm_trace["decision_source"] = "llm_fallback"
        llm_trace["confidence"] = det_trace["confidence"]
        llm_trace["decision_reason"] = (
            f"Fallback LLM (det.confidence={det_trace['confidence']}): "
            f"{det_trace.get('decision_reason', '')}"
        )
        llm_trace["det_trace"] = det_trace
        return llm_trace

    # mode == "shadow": run both, USE the LLM result, attach det_* for comparison
    det_trace = deterministic_decide(case, enrichment)
    llm_trace = get_llm_decision(case, enrichment)
    llm_trace.setdefault("decision_source", "llm")
    llm_trace.setdefault("confidence", "N/A")
    llm_trace.setdefault("decision_reason", "shadow mode (LLM authoritative)")
    llm_trace["det_trace"] = det_trace
    return llm_trace


# ---------------------------------------------------------------------------
# Phase 4 — Google Sheets
# ---------------------------------------------------------------------------


def write_to_sheets(rows: list[list[str]]) -> str | None:
    """Append data rows to the target Google Sheet (creates header if empty)."""
    if IS_DRY_RUN:
        print(f"\n[FASE 4] [DRY-RUN] Simulando escrita de {len(rows)} linha(s) no Sheets — não persistido.")
        return f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit (DRY-RUN)"

    sa_path = settings.google_service_account_path
    if not sa_path or not Path(sa_path).exists():
        print("\n[FASE 4] ⚠ Google Service Account não configurado, pulando escrita.")
        return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)

    print(f"\n[FASE 4] Google Sheets — escrevendo {len(rows)} linhas...")
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_NAME)

    existing = ws.get_all_values()
    current_header = existing[0] if existing else []

    if current_header != HEADER_ROW:
        ws.update(range_name="A1", values=[HEADER_ROW])
        last_cell = rowcol_to_a1(1, len(HEADER_ROW))
        ws.format(f"A1:{last_cell}", {"textFormat": {"bold": True}})
        print(f"  ✓ Header {'corrigido' if current_header else 'criado'} na linha 1")

    ws.append_rows(rows, value_input_option="RAW")

    url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
    print(f"  ✓ Planilha atualizada: {url}")
    return url


# ---------------------------------------------------------------------------
# Phase 5 — Generate Google Docs via Apps Script
# ---------------------------------------------------------------------------


def _format_date_pt() -> str:
    now = datetime.now()
    return f"{now.day} de {MESES_PT[now.month]} de {now.year}"


def _format_cpf(raw: str) -> str:
    d = raw.strip().replace(".", "").replace("-", "").replace("/", "")
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return raw


ACENTUACAO_CIDADES = {
    "SAO PAULO": "SÃO PAULO",
    "MACEIO": "MACEIÓ",
    "MARINGA": "MARINGÁ",
    "CURITIBA": "CURITIBA",
    "GOIANIA": "GOIÂNIA",
    "BRASILIA": "BRASÍLIA",
    "BELEM": "BELÉM",
    "MANAUS": "MANAUS",
    "LONDRINA": "LONDRINA",
    "FLORIANOPOLIS": "FLORIANÓPOLIS",
    "MARILIA": "MARÍLIA",
    "UBERLANDIA": "UBERLÂNDIA",
    "SANTAREM": "SANTARÉM",
    "IMPERATRIZ": "IMPERATRIZ",
    "TERESINA": "TERESINA",
    "JOAO PESSOA": "JOÃO PESSOA",
    "VITORIA": "VITÓRIA",
    "NITEROI": "NITERÓI",
    "MACAPA": "MACAPÁ",
    "SAO GONCALO": "SÃO GONÇALO",
    "SAO JOSE": "SÃO JOSÉ",
    "SAO BERNARDO": "SÃO BERNARDO",
    "SAO CAETANO": "SÃO CAETANO",
    "SAO LUIS": "SÃO LUÍS",
    "ITAJAI": "ITAJAÍ",
    "JUNDIAI": "JUNDIAÍ",
}


def _sanitize(value) -> str:
    """Convert None/empty to 'S/N' and fix common orthography issues."""
    s = str(value) if value is not None else ""
    if not s or s == "None" or s == "null":
        return "S/N"
    return s


def _fix_ortografia(text: str) -> str:
    """Fix missing accents in city/court names."""
    result = text
    for wrong, correct in ACENTUACAO_CIDADES.items():
        if wrong in result.upper():
            import re

            result = re.sub(re.escape(wrong), correct, result, flags=re.IGNORECASE)
    return result


def _norm_for_compare(text: str) -> str:
    """Normalize a string for duplicate detection (lower + collapse whitespace).

    Used to decide if two header fields (vara × órgão) are effectively the
    same human-readable string and one of them should be suppressed to avoid
    repetition in the response letter header.
    """
    import re

    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _dedupe_orgao(vara: str, orgao: str) -> tuple[str, str]:
    """Suppress duplicate header info between Vara/Seccional and Órgão.

    Many ofícios arrive from the dataset with both ``court_tribunal_name``
    (vara_tribunal) and ``organ_name`` (orgao_nome) carrying the same long
    description, e.g. "Poder Judiciário Tribunal de Justiça do Estado do RS
    2ª Vara Cível da Comarca de Bento Gonçalves". The Google Docs template
    renders the two placeholders on consecutive lines, which causes a
    visible repetition that the analyst keeps having to clean up by hand.

    Rule: when the two strings are equivalent (after lower-casing and
    collapsing whitespace) OR when one fully contains the other, keep the
    longest version on ``vara`` and clear ``orgao``. Otherwise return both
    unchanged so legitimate ``Vara ≠ Órgão`` cases keep working.
    """
    norm_vara = _norm_for_compare(vara)
    norm_orgao = _norm_for_compare(orgao)

    if not norm_vara or not norm_orgao:
        return vara, orgao

    if norm_vara == norm_orgao:
        return vara, ""

    if norm_orgao in norm_vara:
        return vara, ""

    if norm_vara in norm_orgao:
        return orgao, ""

    return vara, orgao


def _clean_macro_text(raw: str) -> str:
    """Strip repeated header from macro text and force lowercase start."""
    text = raw
    for prefix in [
        "Em atenção ao ofício judicial, informamos que ",
        "Em atenção ao ofício judicial, ",
        "em atenção ao ofício judicial, informamos que ",
        "em atenção ao ofício judicial, ",
    ]:
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix) :]
            break
    if text and text[0].isupper():
        text = text[0].lower() + text[1:]
    return _fix_ortografia(text)


def build_generate_doc_replacements(
    ref_case: dict,
    inv_results: list[dict],
) -> tuple[dict[str, str], list[str], str]:
    """Build Apps Script replacement map and boldTexts (no HTTP)."""
    tipo = TIPO_MAP.get(ref_case.get("tipo_oficio", ""), ref_case.get("tipo_oficio", ""))
    processo = _sanitize(ref_case.get("numero_processo")) or "DESCONHECIDO"
    doc_name = f"CR-{processo}-{tipo}"

    macro_nao_cliente_coletiva = (
        "em consulta aos dados fornecidos no r. ofício, cumpre-nos informar "
        "que não identificamos em nossa base de clientes o(s) outro(s) "
        "envolvido(s) citado(s)."
    )

    inv_data: list[dict] = []
    macro_ids_seen: set[str] = set()

    for inv in inv_results:
        case = inv["case"]
        llm_trace = inv["llm_trace"]
        enrichment = inv["enrichment"]

        nome = _fix_ortografia(_sanitize(case.get("nome_investigado")))
        cpf_raw = _sanitize(case.get("cpf_cnpj"))
        cpf_fmt = _format_cpf(cpf_raw)
        digits = cpf_raw.replace(".", "").replace("-", "").replace("/", "")
        dt = "CPF" if len(digits) <= 11 else "CNPJ"

        macro_text = llm_trace.get("llm_texto_resposta", "")
        if not macro_text or macro_text == "N/A":
            macro_text = f"Macro: {llm_trace.get('llm_macro_aplicada', 'N/A')}"
        macro_text = _clean_macro_text(macro_text)

        is_nao_cliente = enrichment.get("customers_customer_id") in (
            "NAO_CLIENTE",
            "NOT_FOUND",
        )

        macro_ids_seen.add(llm_trace.get("llm_id_macro", ""))
        inv_data.append(
            {
                "nome": nome,
                "cpf_fmt": cpf_fmt,
                "doc_type": dt,
                "macro": macro_text,
                "is_nao_cliente": is_nao_cliente,
            }
        )

    first = inv_data[0]
    same_macro = len(macro_ids_seen) == 1

    bold_texts: list[str] = []
    for d in inv_data:
        bold_texts.append(f"{d['nome']} - {d['doc_type']} n.º {d['cpf_fmt']}")

    if len(inv_data) == 1:
        combined_name = first["nome"]
        doc_type_label = first["doc_type"]
        doc_field = first["cpf_fmt"]
        combined_macro = first["macro"]
    elif same_macro:
        # Mesmo macro_id NÃO significa mesmo texto — saldos, datas e valores
        # variam por investigado. Por isso replicamos o bloco "Em relação a
        # ..." para cada investigado adicional, usando o texto próprio dele
        # (``d["macro"]``), em vez de descartar.
        combined_name = first["nome"]
        doc_type_label = first["doc_type"]
        doc_field = first["cpf_fmt"]
        extra_parts = [
            f"Em relação a {d['nome']} - {d['doc_type']} n.º {d['cpf_fmt']}, "
            f"informamos que {d['macro']}"
            for d in inv_data[1:]
        ]
        combined_macro = first["macro"] + "\n\n" + "\n\n".join(extra_parts)
    else:
        clientes = [d for d in inv_data if not d["is_nao_cliente"]]
        nao_clientes = [d for d in inv_data if d["is_nao_cliente"]]

        lead = clientes[0] if clientes else first
        combined_name = lead["nome"]
        doc_type_label = lead["doc_type"]
        doc_field = lead["cpf_fmt"]

        if clientes and nao_clientes:
            parts: list[str] = []
            for ci, d in enumerate(clientes):
                if d is lead and ci == 0:
                    parts.append(d["macro"])
                else:
                    parts.append(
                        f"Em relação a {d['nome']} - {d['doc_type']} n.º {d['cpf_fmt']}, "
                        f"informamos que {d['macro']}"
                    )

            nao_cli_names = " e ".join(
                f"{d['nome']} - {d['doc_type']} n.º {d['cpf_fmt']}" for d in nao_clientes
            )
            parts.append(f"Em relação a {nao_cli_names}, {macro_nao_cliente_coletiva}")

            combined_macro = "\n\n".join(parts)
        else:
            extra_parts = [
                f"Em relação a {d['nome']} - {d['doc_type']} n.º {d['cpf_fmt']}, "
                f"informamos que {d['macro']}"
                for d in inv_data[1:]
            ]
            combined_macro = first["macro"] + "\n\n" + "\n\n".join(extra_parts)

    vara = _fix_ortografia(_sanitize(ref_case.get("vara_tribunal")))
    orgao = _fix_ortografia(_sanitize(ref_case.get("orgao_nome")))
    vara, orgao = _dedupe_orgao(vara, orgao)

    replacements = {
        "{{data da elaboração deste documento}}": _format_date_pt(),
        "{{número do ofício}}": _sanitize(ref_case.get("numero_oficio")),
        "{{número do processo}}": processo,
        "{{Vara/Seccional}}": vara,
        "{{Órgão (delegacia/tribunal)}}": orgao,
        "{{NOME DO CLIENTE ATINGIDO}}": combined_name,
        "CPF (CNPJ)": doc_type_label,
        "{{documento do cliente atingido}}": doc_field,
        "{{macro da operação realizada}}": combined_macro,
    }

    return replacements, bold_texts, doc_name


def generate_doc(ref_case: dict, inv_results: list[dict]) -> str | None:
    """Call the Apps Script to create a filled letter in the Drive folder.

    Args:
        ref_case: Reference case dict (common fields: processo, vara, orgao, etc.).
        inv_results: List of dicts, each with keys ``case``, ``enrichment``, ``llm_trace``
            representing one investigated person in this ofício.

    Returns the Google Doc URL on success, or None on failure.
    """
    if not settings.apps_script_url:
        print("    ⚠ APPS_SCRIPT_URL não configurada, pulando geração de doc.")
        return None

    if IS_DRY_RUN:
        fake_id = f"DRYRUN-{int(time.time()*1000)}"
        fake_url = f"https://docs.google.com/document/d/{fake_id}/edit"
        print(f"    [DRY-RUN] Simulando generate_doc → {fake_url}")
        return fake_url

    replacements, bold_texts, doc_name = build_generate_doc_replacements(ref_case, inv_results)

    payload = {
        "templateId": settings.google_template_doc_id,
        "folderId": settings.google_drive_folder_id,
        "docName": doc_name,
        "subfolderName": _sanitize(ref_case.get("numero_processo")) or "DESCONHECIDO",
        "replacements": replacements,
        "boldTexts": bold_texts,
    }

    max_retries = 3
    backoff = [5, 15, 30]
    last_error: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.post(
                settings.apps_script_url,
                json=payload,
                timeout=90,
                follow_redirects=True,
            )
            resp.raise_for_status()
            result = resp.json()

            if "error" in result:
                last_error = str(result["error"])
                if attempt < max_retries:
                    wait = backoff[attempt - 1]
                    print(
                        f"    [DOC] Tentativa {attempt}/{max_retries} falhou ({last_error}), retentando em {wait}s..."
                    )
                    time.sleep(wait)
                    continue
                print(f"    ✗ Apps Script erro: {last_error}")
                return None

            doc_url = result.get("docUrl", "")
            print(f"    ✓ Doc criado: {doc_url}")
            return doc_url

        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                wait = backoff[attempt - 1]
                print(
                    f"    [DOC] Tentativa {attempt}/{max_retries} falhou ({last_error}), retentando em {wait}s..."
                )
                time.sleep(wait)
                continue
            print(f"    ✗ Erro ao gerar doc após {max_retries} tentativas: {last_error}")
            return None

    return None


def group_cases_by_oficio(cases: list[dict]) -> list[dict]:
    """Group flat case rows by id_oficio, deduplicating investigados."""
    cases_sorted = sorted(cases, key=lambda c: c.get("id_oficio", ""))
    grouped: list[dict] = []
    for id_oficio, group_iter in groupby(cases_sorted, key=lambda c: c.get("id_oficio", "")):
        investigados = list(group_iter)
        seen: set[tuple[str, str]] = set()
        unique: list[dict] = []
        for inv in investigados:
            key = (inv.get("nome_investigado", ""), inv.get("cpf_cnpj", ""))
            if key not in seen:
                seen.add(key)
                unique.append(inv)
        grouped.append(
            {
                "id_oficio": id_oficio,
                "investigados": unique,
                "ref_case": unique[0],
            }
        )
    return grouped


# ---------------------------------------------------------------------------
# Slack Notifier
# ---------------------------------------------------------------------------

# Error → recommended action mapping is defined once in lexia.monitoring
# (single source of truth for both the Slack notifier and the runbook).
_ERROR_ACTION_MAP = {cat.value: action for cat, action in ERROR_ACTIONS.items()}


class SlackNotifier:
    """Posts structured updates to a Slack thread. All methods are no-op safe."""

    _DIVIDER = "─" * 36
    _THREAD_TS_FILE = Path(__file__).resolve().parent.parent / "logs" / ".slack_thread_ts"

    def __init__(self, token: str, channel_id: str, enabled: bool = True):
        self._enabled = enabled and bool(token)
        self._channel = channel_id
        self._thread_ts: str | None = None
        self._parent_ts: str | None = None
        self._case_counter = 0
        self._client = SlackWebClient(token=token) if self._enabled else None

    def _post(self, text: str, *, thread: bool = True) -> str | None:
        if not self._enabled or not self._client:
            return None
        try:
            kwargs: dict = {"channel": self._channel, "text": text, "unfurl_links": False}
            if thread and self._thread_ts:
                kwargs["thread_ts"] = self._thread_ts
            resp = self._client.chat_postMessage(**kwargs)
            return resp.get("ts")
        except Exception as e:
            print(f"  [SLACK WARN] Falha ao postar: {e}")
            return None

    def _load_today_thread(self) -> str | None:
        """Load thread_ts from cache file if it belongs to today."""
        try:
            if not self._THREAD_TS_FILE.exists():
                return None
            data = json.loads(self._THREAD_TS_FILE.read_text())
            if data.get("date") == datetime.now().strftime("%Y-%m-%d"):
                return data.get("thread_ts")
        except Exception:
            pass
        return None

    def _save_thread_ts(self, thread_ts: str):
        """Persist today's thread_ts to cache file."""
        try:
            self._THREAD_TS_FILE.parent.mkdir(parents=True, exist_ok=True)
            self._THREAD_TS_FILE.write_text(
                json.dumps(
                    {
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "thread_ts": thread_ts,
                        "channel": self._channel,
                    }
                )
            )
        except Exception:
            pass

    def start_thread(
        self,
        total: int,
        bloqueio: int,
        desbloqueio: int,
        transferencia: int,
        skipped: int = 0,
    ):
        if not self._enabled:
            return
        self._total = total
        today = datetime.now().strftime("%d/%m/%Y")

        existing_ts = self._load_today_thread()
        is_resumed = False
        if existing_ts:
            self._thread_ts = existing_ts
            self._parent_ts = existing_ts
            is_resumed = True
            print(f"  [SLACK] Reutilizando thread do dia ({existing_ts})")
        else:
            parent_text = (
                f"*[LexIA - Confecção de Carta-Resposta Judicial via IA - {today}]* :thread:"
            )
            self._parent_ts = self._post(parent_text, thread=False)
            self._thread_ts = self._parent_ts
            if self._thread_ts:
                self._save_thread_ts(self._thread_ts)
                print(f"  [SLACK] Nova thread criada ({self._thread_ts})")

        if not self._thread_ts:
            return

        # Em retomadas (thread reutilizada), pular o post de "Execução iniciada"
        # para não poluir a thread já em andamento.
        if is_resumed:
            return

        type_lines = []
        if bloqueio:
            type_lines.append(f"  :lock:  Bloqueio — *{bloqueio}*")
        if desbloqueio:
            type_lines.append(f"  :unlock:  Desbloqueio — *{desbloqueio}*")
        if transferencia:
            type_lines.append(f"  :arrows_counterclockwise:  Transferência — *{transferencia}*")
        type_block = "\n".join(type_lines)

        skipped_line = ""
        if skipped:
            skipped_line = f"\n:fast_forward:  _{skipped} caso(s) já processado(s) — pulados_"

        start_msg = (
            f":hourglass_flowing_sand:  *Execução iniciada*\n"
            f"\n"
            f"*Casos a processar:* {total}\n"
            f"{type_block}{skipped_line}\n"
            f"\n"
            f"{self._DIVIDER}"
        )
        self._post(start_msg)

    def notify_case_success(
        self,
        processo: str,
        tipo: str,
        inv_results: list[dict],
        doc_url: str | None,
        lexia_id: str = "",
    ):
        """Notify success for an ofício with one or more investigados."""
        self._case_counter += 1
        doc_link = f"\n:page_facing_up:  <{doc_url}|Abrir Carta-Resposta>" if doc_url else ""

        tipo_emoji = {
            "BLOQUEIO": ":lock:",
            "DESBLOQUEIO": ":unlock:",
            "TRANSFERÊNCIA": ":arrows_counterclockwise:",
        }.get(tipo, ":question:")

        inv_lines = []
        for inv in inv_results:
            nome = inv["case"].get("nome_investigado", "N/A")
            macro = inv["llm_trace"].get("llm_id_macro", "?")
            inv_lines.append(f">  :bust_in_silhouette:  {nome} — Macro *{macro}*")
        inv_block = "\n".join(inv_lines)

        msg = (
            f":white_check_mark:  *Caso {self._case_counter}/{self._total}*  `{lexia_id}`\n"
            f"\n"
            f">  *Processo:* `{processo}`\n"
            f">  {tipo_emoji}  *Tipo:* {tipo}\n"
            f">  *Investigados:* {len(inv_results)}\n"
            f"{inv_block}"
            f"{doc_link}"
        )
        self._post(msg)

    def notify_case_error(
        self,
        processo: str,
        tipo: str,
        category: ErrorCategory,
        detail: str = "",
        lexia_id: str = "",
        investigados: list[str] | None = None,
    ):
        self._case_counter += 1
        action = ERROR_ACTIONS.get(category, "Investigar logs do pipeline.")
        detail_line = f"\n>  *Detalhe:* {detail}" if detail else ""

        tipo_emoji = {
            "BLOQUEIO": ":lock:",
            "DESBLOQUEIO": ":unlock:",
            "TRANSFERÊNCIA": ":arrows_counterclockwise:",
        }.get(tipo, ":question:")

        inv_line = ""
        if investigados:
            inv_clean = [str(n) if n is not None else "(sem nome)" for n in investigados]
            inv_line = f"\n>  *Investigados:* {', '.join(inv_clean)}"

        msg = (
            f":x:  *Caso {self._case_counter}/{self._total} — FALHA*  `{lexia_id}`\n"
            f"\n"
            f">  *Processo:* `{processo}`\n"
            f">  {tipo_emoji}  *Tipo:* {tipo}{inv_line}\n"
            f">  *Categoria:* `{category.value}`{detail_line}\n"
            f"\n"
            f":warning:  *Ação necessária:* {action}"
        )
        self._post(msg)

    def notify_case_certs_missing(
        self,
        processo: str,
        tipo: str,
        lexia_id: str = "",
        investigados: list[str] | None = None,
    ):
        self.notify_case_error(
            processo,
            tipo,
            ErrorCategory.CERTS_MISSING,
            "Certificados mTLS não encontrados no ambiente.",
            lexia_id,
            investigados=investigados,
        )

    def finish(self, summary: RunSummary, slo: SLOReport):
        """Post the run wrap-up with totals, error breakdown and SLO status."""
        if not self._enabled:
            return

        duration_secs = summary.total_duration_secs
        mins = int(duration_secs // 60)
        secs = int(duration_secs % 60)
        duration_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"

        if summary.total > 0:
            avg_str = f"{summary.avg_duration_secs:.1f}s por ofício"
        else:
            avg_str = "—"

        has_issues = summary.errors or summary.certs_missing or not slo.healthy
        if not slo.healthy:
            status_emoji = ":rotating_light:"
            status_text = "Concluída com SLO estourado"
        elif has_issues:
            status_emoji = ":warning:"
            status_text = "Concluída com pendências"
        else:
            status_emoji = ":white_check_mark:"
            status_text = "Concluída com sucesso"

        result_lines = [f"  :white_check_mark:  Sucesso — *{summary.succeeded}*"]
        if summary.errors:
            result_lines.append(f"  :x:  Erros — *{summary.errors}*")
        if summary.certs_missing:
            result_lines.append(f"  :no_entry_sign:  Certs ausentes — *{summary.certs_missing}*")
        result_block = "\n".join(result_lines)

        # Erros por categoria (só aparece se houver erros)
        errors_by_cat = summary.errors_by_category
        cat_block = ""
        if errors_by_cat:
            cat_lines = [f"  •  `{cat}` — *{count}*" for cat, count in sorted(errors_by_cat.items())]
            cat_block = "\n*Erros por categoria:*\n" + "\n".join(cat_lines) + "\n"

        # Bloco SLO — por padrão FORA da postagem do Slack (decisão de produto:
        # thread limpa para o time de Ops). Os SLOs continuam sendo calculados
        # e persistidos em logs/run-summary-{YYYY-MM-DD}.json para o on-call.
        # Para reativar a postagem (ex.: durante investigação), basta setar:
        #     LEXIA_SHOW_SLO_BLOCK=1
        # Compat: LEXIA_SUPPRESS_SLO_BLOCK=1 (legado) continua funcionando como
        # no-op explícito, sem efeito sobre o novo default.
        show_slo = os.environ.get("LEXIA_SHOW_SLO_BLOCK", "").lower() in ("1", "true", "yes")
        if show_slo:
            p95 = summary.percentile_duration_secs(95)
            slo_lines = [
                f"  •  p95 de duração: *{p95:.1f}s* (alvo ≤ {slo.targets.p95_seconds:.0f}s)",
                f"  •  Taxa de erro: *{summary.error_rate:.1%}* (alvo ≤ {slo.targets.error_rate:.0%})",
                f"  •  Uso de fallback IA: *{summary.fallback_rate:.1%}* (alvo ≤ {slo.targets.fallback_rate:.0%})",
            ]
            slo_block = "\n*SLOs (JUD-1995):*\n" + "\n".join(slo_lines)

            violations_block = ""
            if not slo.healthy:
                v_lines = [f"  :rotating_light:  {v.message}" for v in slo.violations]
                violations_block = "\n\n*SLO estourado — investigar:*\n" + "\n".join(v_lines)
        else:
            slo_block = ""
            violations_block = ""

        msg = (
            f"{self._DIVIDER}\n"
            f"\n"
            f"{status_emoji}  *{status_text}*\n"
            f"\n"
            f"*Resultado:*\n"
            f"{result_block}\n"
            f"{cat_block}"
            f"\n"
            f":stopwatch:  *Tempo total:* {duration_str}\n"
            f":hourglass_flowing_sand:  *Tempo médio:* {avg_str}\n"
            f"{slo_block}"
            f"{violations_block}"
        )
        self._post(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    import time as _time

    t_start = _time.monotonic()
    run_summary = RunSummary(
        decision_mode=settings.decision_mode,
        days_back=int(os.environ.get("DAYS_BACK", os.environ.get("LEXIA_DAYS_BACK", "0")) or 0),
    )

    limit = int(os.environ.get("LEXIA_LIMIT", "0")) or None
    processes = TARGET_PROCESSES or None

    print("=" * 70)
    print("LexIA CR — Pipeline de Rastreabilidade")
    if processes:
        print(f"Processos: {len(processes)}")
    else:
        print(f"Modo: últimos casos (LIMIT {limit or 'ALL'})")
    print(f"Timestamp: {datetime.now(UTC).isoformat()}")
    print("=" * 70)

    # Slack notifier
    slack = SlackNotifier(
        token=settings.slack_bot_token,
        channel_id=settings.slack_channel_id,
        enabled=settings.slack_notify_enabled,
    )

    # Phase 1
    cases = fetch_cases_from_databricks(processes=processes, limit=limit)
    if not cases:
        print("\n❌ Nenhum caso encontrado no Databricks.")
        sys.exit(1)

    # Deduplication — skip already-processed ofícios
    print("\n[DEDUP] Verificando ofícios já processados...")
    if IS_DRY_RUN:
        print("  [DRY-RUN] Pulando dedup — todos os casos do range serão simulados.")
        already_done = set()
    else:
        already_done = fetch_processed_oficios()
    skipped_count = 0
    if already_done:
        before = len(cases)
        cases = [c for c in cases if c.get("id_oficio") not in already_done]
        skipped_count = before - len(cases)
        if skipped_count:
            print(f"  ✓ {skipped_count} linha(s) de ofícios já processados — pulando")
        if not cases:
            print("\n✓ Todos os ofícios já foram processados anteriormente.")
            sys.exit(0)
    run_summary.skipped_already_processed = skipped_count

    # Group rows by id_oficio (1 ofício = 1 carta-resposta)
    grouped_cases = group_cases_by_oficio(cases)

    print(f"  → {len(grouped_cases)} ofício(s) a processar ({len(cases)} linha(s) agrupadas)")

    # Count by type per ofício for Slack breakdown
    n_bloqueio = sum(
        1
        for g in grouped_cases
        if TIPO_MAP.get(g["ref_case"].get("tipo_oficio", ""), "") == "BLOQUEIO"
    )
    n_desbloqueio = sum(
        1
        for g in grouped_cases
        if TIPO_MAP.get(g["ref_case"].get("tipo_oficio", ""), "") == "DESBLOQUEIO"
    )
    n_transferencia = sum(
        1
        for g in grouped_cases
        if TIPO_MAP.get(g["ref_case"].get("tipo_oficio", ""), "") == "TRANSFERÊNCIA"
    )

    slack.start_thread(
        total=len(grouped_cases),
        bloqueio=n_bloqueio,
        desbloqueio=n_desbloqueio,
        transferencia=n_transferencia,
        skipped=skipped_count,
    )

    all_rows: list[list[str]] = []

    for i, group in enumerate(grouped_cases, 1):
        id_oficio = group["id_oficio"]
        ref = group["ref_case"]
        investigados = group["investigados"]
        processo = ref.get("numero_processo", "?")
        tipo = TIPO_MAP.get(ref.get("tipo_oficio", ""), ref.get("tipo_oficio", ""))
        lexia_id = generate_lexia_id()

        # Per-ofício timing (JUD-1995): start_at + duration go to the spreadsheet
        # so we can compute p95 across runs and feed the SLO checker.
        case_started_at = datetime.now(UTC).isoformat()
        t_case_start = _time.monotonic()

        print(f"\n{'—' * 70}")
        print(f"[{i}/{len(grouped_cases)}] {lexia_id} | Ofício: {id_oficio[:12]}...")
        print(f"  Processo: {processo} ({tipo})")
        print(f"  Investigados: {len(investigados)}")

        inv_results: list[dict] = []
        overall_status = "success"

        for j, inv in enumerate(investigados, 1):
            nome = inv.get("nome_investigado", "N/A")
            cpf = inv.get("cpf_cnpj", "N/A")
            print(f"\n  [{j}/{len(investigados)}] Investigado: {nome} | {cpf}")

            # Phase 2 — Enrich
            print("    [FASE 2] APIs Nubank — enriquecimento...")
            enrichment = enrich_case(inv)
            print(f"      Waze shard:    {enrichment['waze_shard']}")
            print(f"      Customer ID:   {enrichment['customers_customer_id']}")
            print(f"      NuConta:       {enrichment['nuconta_status'][:80]}")
            print(f"      Saldo NuConta: {enrichment['nuconta_saldo'][:80]}")
            print(f"      Crebito:       {enrichment['crebito_cartoes'][:80]}")
            print(f"      Rayquaza:      {enrichment['rayquaza_saldo'][:80]}")
            print(f"      Petrificus:    {enrichment['petrificus_bloqueios'][:80]}")
            print(f"      Mario-Box:     {enrichment['mario_box_caixinhas'][:80]}")
            print(f"      Dados Banc.:   {enrichment['dados_bancarios'][:80]}")

            # Phase 3 — Decision (LLM, deterministic, shadow or hybrid)
            print(f"    [FASE 3] Decisão (modo={settings.decision_mode})...")
            llm_trace = get_decision(inv, enrichment)
            print(f"      Macro:         {llm_trace['llm_macro_aplicada']}")
            print(f"      ID Macro:      {llm_trace['llm_id_macro']}")
            print(
                f"      Source:        {llm_trace.get('decision_source', '?')} "
                f"(confidence={llm_trace.get('confidence', 'N/A')})"
            )
            print(f"      Observações:   {llm_trace['llm_observacoes'][:100]}")
            det_trace = llm_trace.get("det_trace") or {}
            if det_trace and det_trace.get("decision_source") not in ("skipped", None):
                print(
                    f"      [DET] macro={det_trace.get('llm_id_macro')} "
                    f"conf={det_trace.get('confidence')} "
                    f"reason={det_trace.get('decision_reason', '')[:80]}"
                )

            if enrichment["waze_shard"] == "CERTS_NAO_CONFIGURADOS":
                overall_status = "certs_missing"
            if llm_trace["llm_macro_aplicada"].startswith("ERRO"):
                overall_status = "error"

            inv_results.append(
                {
                    "case": inv,
                    "enrichment": enrichment,
                    "llm_trace": llm_trace,
                }
            )

        # Phase 5 — Generate single Google Doc for this ofício
        print("\n  [FASE 5] Google Drive — gerando carta-resposta...")
        doc_url = generate_doc(ref, inv_results)

        if not doc_url and overall_status == "success":
            overall_status = "error"

        # Categorize the failure (JUD-1995). Done once per ofício so that the
        # spreadsheet, the Slack notification and the run-summary all use the
        # same canonical category.
        case_enrichments = [r["enrichment"] for r in inv_results]
        error_category = categorize_case_error(
            enrichments=case_enrichments,
            inv_results=inv_results,
            doc_url=doc_url,
            overall_status=overall_status,
        )

        case_duration = _time.monotonic() - t_case_start

        # Slack per-ofício notification (blindado: falha de Slack NUNCA derruba pipeline)
        inv_names = [
            (inv.get("nome_investigado") or "(sem nome)") for inv in investigados
        ]
        try:
            if overall_status == "success":
                slack.notify_case_success(
                    processo,
                    tipo,
                    inv_results,
                    doc_url,
                    lexia_id,
                )
            elif overall_status == "certs_missing":
                slack.notify_case_certs_missing(processo, tipo, lexia_id, investigados=inv_names)
            else:
                first_obs = inv_results[0]["llm_trace"].get("llm_observacoes", "")[:200]
                slack.notify_case_error(
                    processo,
                    tipo,
                    error_category,
                    first_obs,
                    lexia_id,
                    investigados=inv_names,
                )
        except Exception as _slack_err:
            print(f"  [SLACK WARN] Notificação falhou (pipeline continua): {_slack_err}")

        # Build single Sheet row for this ofício (concatenated fields)
        info_sol = ref.get("info_solicitada", "")
        if isinstance(info_sol, list):
            info_sol = ", ".join(str(x) for x in info_sol)

        def _s(v: object) -> str:
            return "" if v is None else str(v)

        all_names = " | ".join(_s(r["case"].get("nome_investigado")) or "(sem nome)" for r in inv_results)
        all_cpfs = " | ".join(_s(r["case"].get("cpf_cnpj")) for r in inv_results)
        all_macros = " | ".join(_s(r["llm_trace"].get("llm_macro_aplicada")) for r in inv_results)
        all_macro_ids = " | ".join(_s(r["llm_trace"].get("llm_id_macro")) for r in inv_results)
        all_macro_texts = " | ".join(_s(r["llm_trace"].get("llm_texto_resposta")) for r in inv_results)
        all_obs = " | ".join(_s(r["llm_trace"].get("llm_observacoes")) for r in inv_results)
        all_raw = " | ".join(_s(r["llm_trace"].get("llm_raw_response")) for r in inv_results)

        # Deterministic engine bookkeeping (per ofício, joined across investigados)
        det_traces = [r["llm_trace"].get("det_trace") or {} for r in inv_results]
        det_id_macro = " | ".join(_s(t.get("llm_id_macro")) for t in det_traces)
        det_macro_aplicada = " | ".join(_s(t.get("llm_macro_aplicada")) for t in det_traces)
        det_texto = " | ".join(_s(t.get("llm_texto_resposta")) for t in det_traces)[:2000]
        det_source = " | ".join(_s(t.get("decision_source")) or "skipped" for t in det_traces)
        det_confidence = " | ".join(_s(t.get("confidence")) or "N/A" for t in det_traces)
        det_reason = " | ".join(_s(t.get("decision_reason"))[:200] for t in det_traces)

        # Comparisons LLM vs deterministic (only meaningful in shadow mode)
        match_flags: list[str] = []
        sim_scores: list[str] = []
        for r in inv_results:
            llm_t = r["llm_trace"]
            det_t = llm_t.get("det_trace") or {}
            if det_t.get("decision_source") in ("skipped", None, ""):
                match_flags.append("N/A")
                sim_scores.append("N/A")
                continue
            same_macro = (det_t.get("llm_id_macro") or "") == (llm_t.get("llm_id_macro") or "")
            match_flags.append("TRUE" if same_macro else "FALSE")
            sim = _text_similarity(
                det_t.get("llm_texto_resposta", ""),
                llm_t.get("llm_texto_resposta", ""),
            )
            sim_scores.append(f"{sim:.3f}")
        det_match_macro = " | ".join(match_flags)
        det_match_text_similarity = " | ".join(sim_scores)

        first_enr = inv_results[0]["enrichment"]
        all_vals = " | ".join(str(r["case"].get("valor_solicitado", "")) for r in inv_results)
        all_is_cli = " | ".join(str(r["case"].get("is_cliente_nu", "")) for r in inv_results)

        row = [
            lexia_id,
            str(id_oficio),
            str(ref.get("numero_processo", "")),
            tipo,
            str(info_sol),
            str(ref.get("numero_oficio", "")),
            all_names,
            all_cpfs,
            str(len(investigados)),
            str(ref.get("vara_tribunal", "")),
            str(ref.get("orgao_nome", "")),
            all_vals,
            all_is_cli,
            str(ref.get("data_recebimento", "")),
            str(first_enr["waze_shard"]),
            str(first_enr["customers_customer_id"]),
            str(first_enr["nuconta_status"]),
            str(first_enr["nuconta_saldo"]),
            str(first_enr["mario_box_caixinhas"]),
            str(first_enr["crebito_cartoes"]),
            str(first_enr["rayquaza_saldo"]),
            str(first_enr["petrificus_bloqueios"]),
            str(first_enr["dados_bancarios"]),
            all_macros,
            all_macro_ids,
            all_macro_texts,
            all_obs,
            all_raw[:2000],
            doc_url or "NAO_GERADO",
            overall_status,
            datetime.now(UTC).isoformat(),
            case_started_at,
            f"{case_duration:.2f}",
            error_category.value,
            det_id_macro,
            det_macro_aplicada,
            det_texto,
            det_source,
            det_match_macro,
            det_match_text_similarity,
            det_confidence,
            det_reason,
        ]
        all_rows.append(row)

        # Feed the run summary (JUD-1995) — one record per ofício.
        first_source = ""
        for r in inv_results:
            src = (r.get("llm_trace") or {}).get("decision_source") or ""
            if src:
                first_source = src
                break
        run_summary.add(
            CaseRecord(
                lexia_id=lexia_id,
                id_oficio=str(id_oficio),
                numero_processo=str(ref.get("numero_processo", "")),
                tipo_oficio=tipo,
                status=overall_status,
                error_category=error_category,
                duration_secs=round(case_duration, 2),
                decision_source=first_source,
            )
        )

    # Phase 4
    try:
        write_to_sheets(all_rows)
    except Exception as e:
        print(f"\n[FASE 4] ⚠ Erro ao gravar no Sheets (não impede finalização): {e}")

    # Finalize the run summary and check SLOs (JUD-1995).
    run_summary.mark_finished()
    slo_report = compute_slo_report(run_summary)

    print(f"\n{'=' * 70}")
    print(f"✓ Pipeline concluído — {run_summary.total} ofício(s) processados")
    print(
        f"  Success: {run_summary.succeeded} | "
        f"Certs missing: {run_summary.certs_missing} | "
        f"Errors: {run_summary.errors}"
    )
    if run_summary.errors_by_category:
        print("  Erros por categoria:")
        for cat, count in sorted(run_summary.errors_by_category.items()):
            print(f"    • {cat}: {count}")
    print(
        f"  Duração — total: {run_summary.total_duration_secs:.1f}s | "
        f"avg: {run_summary.avg_duration_secs:.1f}s | "
        f"p95: {run_summary.percentile_duration_secs(95):.1f}s"
    )
    print(
        f"  SLOs (alvos: p95≤{slo_report.targets.p95_seconds:.0f}s, "
        f"erro≤{slo_report.targets.error_rate:.0%}, "
        f"fallback≤{slo_report.targets.fallback_rate:.0%}) → "
        f"{'OK' if slo_report.healthy else 'ESTOURADO'}"
    )
    if not slo_report.healthy:
        for v in slo_report.violations:
            print(f"    ! {v.message}")
    print("=" * 70)

    # Persist run-summary JSON for the on-call to inspect (JUD-1995 AC).
    try:
        logs_dir = Path(__file__).resolve().parent.parent / "logs"
        out_path = write_run_summary_json(run_summary, slo_report, logs_dir)
        print(f"  Run summary salvo em: {out_path}")
    except Exception as e:
        print(f"  [WARN] Falha ao salvar run-summary JSON: {e}")

    # Total wall-clock duration (kept for consistency with previous behavior).
    _ = _time.monotonic() - t_start
    slack.finish(run_summary, slo_report)


if __name__ == "__main__":
    main()
