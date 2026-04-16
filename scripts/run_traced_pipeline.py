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
from datetime import datetime, timezone
from pathlib import Path

import gspread
import httpx
from google.oauth2.service_account import Credentials
from openai import OpenAI
from slack_sdk import WebClient as SlackWebClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lexia.config import settings

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TARGET_PROCESSES = os.environ.get("LEXIA_TARGET_PROCESSES", "").split(",") if os.environ.get("LEXIA_TARGET_PROCESSES") else [
    # Set via env var LEXIA_TARGET_PROCESSES (comma-separated) or edit this list
]

SPREADSHEET_ID = os.environ.get("LEXIA_SPREADSHEET_ID", "")
SHEET_NAME = "Relatorio_final"

MESES_PT = [
    "", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
]

HEADER_ROW = [
    "lexia_id",
    "numero_processo",
    "tipo_oficio",
    "info_solicitada",
    "numero_oficio",
    "nome_investigado",
    "cpf_cnpj",
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


def fetch_processed_processes() -> set[str]:
    """Read the spreadsheet and return process numbers already completed."""
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
        processo_idx = header.index("numero_processo") if "numero_processo" in header else 1
        status_idx = header.index("status_execucao") if "status_execucao" in header else -2
        return {
            row[processo_idx]
            for row in records[1:]
            if len(row) > max(processo_idx, status_idx)
            and row[status_idx] == "success"
        }
    except Exception as e:
        print(f"  [WARN] Não foi possível ler processos já concluídos: {e}")
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
- [Não há seguro de vida / Não há investimentos / Não há criptoativos / Não há conta PJ / Não há conta Global]\n\n\
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
WITH ranked AS (
    SELECT
        ext.official_letter_extraction__created_at               AS data_recebimento,
        ol.official_letter__id                                    AS id_oficio,
        ol.official_letter__type                                  AS tipo_oficio,
        ol.official_letter__status                                AS status_oficio,
        ext.official_letter_extraction__craft_document_number      AS numero_oficio,
        ext.official_letter_extraction__process_document_number    AS numero_processo,
        ext.official_letter_extraction__court_tribunal_name        AS vara_tribunal,
        ext.official_letter_extraction__organ_name                 AS orgao_nome,
        name_pii.investigated_information__name                    AS nome_investigado,
        cpf_pii.investigated_information__cpf_cnpj                 AS cpf_cnpj,
        inv.investigated_information__requested_value              AS valor_solicitado,
        inv.investigated_information__customer_id                  AS customer_id,
        inv.investigated_information__is_customer                  AS is_cliente_nu,
        ext.official_letter_extraction__is_reiteration             AS is_reiteracao,
        ext.official_letter_extraction__confirmed_or_rejected_at   AS triado_em,
        ext.official_letter_extraction__confirmed_or_rejected_by   AS triado_por,
        ext.official_letter_extraction__requested_information       AS info_solicitada,

        ROW_NUMBER() OVER (
            PARTITION BY ext.official_letter_extraction__process_document_number
            ORDER BY ext.official_letter_extraction__confirmed_or_rejected_at DESC
        ) AS rn

    FROM etl.br__dataset.jud_athena_official_letter_extractions ext

    INNER JOIN etl.br__dataset.jud_athena_submissions sub
        ON ext.official_letter_extraction__submission_id = sub.submission__id

    INNER JOIN etl.br__dataset.jud_athena_official_letters ol
        ON ol.official_letter__submission_id = sub.submission__id

    LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
        ON inv.investigated_information__id = ol.investigated_information__id[0]

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_name_pii name_pii
        ON name_pii.hash = inv.investigated_information__name

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_cpf_cnpj_pii cpf_pii
        ON cpf_pii.hash = inv.investigated_information__cpf_cnpj

    WHERE ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
      AND ol.official_letter__type IN (
          'official_letter_type__block',
          'official_letter_type__dismiss',
          'official_letter_type__transfer'
      )
      AND ext.official_letter_extraction__process_document_number IS NOT NULL
      {extra_filter}
)

SELECT *
FROM ranked
WHERE rn = 1
ORDER BY data_recebimento DESC
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
        days = int(os.environ.get("DAYS_BACK", "12"))
        extra = f"AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL {days} DAYS"

    query = QUERY_BASE.format(extra_filter=extra)
    if limit:
        query = query.rstrip().rstrip(";")
        query += f"\nLIMIT {limit}"

    label = f"{len(processes)} processos" if processes else f"últimos casos (LIMIT {limit or 'ALL'})"
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

    cases = [dict(zip(columns, row)) for row in rows]
    print(f"  ✓ {len(cases)} registros retornados (deduplicados)")
    for c in cases:
        print(f"    • {c.get('numero_processo')} | {TIPO_MAP.get(c.get('tipo_oficio',''), c.get('tipo_oficio',''))} | {c.get('nome_investigado','N/A')}")

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
    return NU_CERT_PATH.exists() and NU_KEY_PATH.exists() and NU_TOKEN_PATH.exists()


def _get_nu_client() -> httpx.Client:
    """Build an httpx client with nucli certs + cached bearer token."""
    token = NU_TOKEN_PATH.read_text().strip()
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.load_cert_chain(str(NU_CERT_PATH), str(NU_KEY_PATH))
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    return httpx.Client(
        verify=ssl_ctx,
        timeout=30,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )


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
        for k in ["waze_shard", "customers_customer_id", "nuconta_status",
                   "nuconta_saldo", "crebito_cartoes", "rayquaza_saldo",
                   "petrificus_bloqueios", "dados_bancarios"]:
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

        waze_resp = client.post(waze_url, json=waze_payload)
        waze_resp.raise_for_status()
        shard = waze_resp.json().get("shard")
        trace["waze_shard"] = shard or "NOT_FOUND"

        if not shard:
            client.close()
            return trace

        # Customers — /company/ para PJ, /person/ para PF
        if is_pj:
            cust_url = f"https://prod-{shard}-customers.nubank.com.br/api/customers/company/find-by-tax-id"
        else:
            cust_url = f"https://prod-{shard}-customers.nubank.com.br/api/customers/person/find-by-tax-id"

        cust_resp = client.post(
            cust_url,
            json={"tax_id": clean_id},
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
            sa_resp = client.get(
                f"https://prod-{shard}-savings-accounts.nubank.com.br/api/customer/{customer_id}/savings-account"
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
                diablo_resp = client.get(
                    f"https://prod-{shard}-diablo.nubank.com.br/api/savings-accounts/{savings_account_id}/balance/{today}"
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
            facade_resp = client.get(
                f"https://prod-{shard}-facade.nubank.com.br/api/customers/{customer_id}/account"
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
            assets_resp = client.get(
                f"https://prod-{shard}-rayquaza.nubank.com.br/api/customers/{customer_id}/available-assets"
            )
            if assets_resp.status_code == 404:
                trace["rayquaza_saldo"] = "SEM_ATIVOS"
            else:
                assets_resp.raise_for_status()
                assets_data = assets_resp.json()
                assets_list = assets_data if isinstance(assets_data, list) else assets_data.get("assets", [])
                trace["assets"] = assets_list

                caixinhas_total = sum(
                    float(a.get("available_amount", 0) or 0)
                    for a in assets_list
                    if a.get("kind") == "liquid_deposit"
                )
                total_seizable = sum(
                    float(a.get("available_amount", 0) or 0)
                    for a in assets_list
                )
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
            blocks_resp = client.get(
                f"https://prod-{shard}-petrificus-parcialus.nubank.com.br/api/customers/{customer_id}/freeze-orders"
            )
            if blocks_resp.status_code == 404:
                trace["petrificus_bloqueios"] = "SEM_BLOQUEIOS"
            else:
                blocks_resp.raise_for_status()
                blocks_data = blocks_resp.json()
                blocks_list = blocks_data if isinstance(blocks_data, list) else blocks_data.get("freeze_orders", [])
                trace["blocks"] = blocks_list
                trace["petrificus_bloqueios"] = json.dumps(
                    [{"status": b.get("status"), "amount": b.get("amount")} for b in blocks_list],
                    ensure_ascii=False,
                )
        except Exception as e:
            trace["petrificus_bloqueios"] = f"ERRO: {e}"

        # Mario-Box — Caixinhas metadata (nomes, sem saldo — saldo vem do Rayquaza)
        try:
            mb_resp = client.get(
                f"https://prod-{shard}-mario-box.nubank.com.br/api/customers/{customer_id}/money-boxes"
            )
            if mb_resp.status_code == 404:
                trace["mario_box_caixinhas"] = "SEM_CAIXINHAS"
            else:
                mb_resp.raise_for_status()
                mb_data = mb_resp.json()
                boxes = mb_data if isinstance(mb_data, list) else mb_data.get("money_boxes", mb_data.get("moneyBoxes", []))
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
                ba_resp = client.get(
                    "https://prod-global-bank-accounts-widget-provider.nubank.com.br"
                    "/api/savings-accounts/resources/country-data",
                    params={
                        "customer-id": customer_id,
                        "savings-account-id": savings_account_id,
                    },
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
        for k in ["waze_shard", "customers_customer_id", "crebito_cartoes",
                   "rayquaza_saldo", "petrificus_bloqueios", "dados_bancarios"]:
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

    THRESHOLD_INFIMO = 10.0
    saldo_disponivel_raw = nuconta_saldo.get("available", "0")
    try:
        saldo_float = float(saldo_disponivel_raw) if saldo_disponivel_raw else 0.0
    except (ValueError, TypeError):
        saldo_float = 0.0

    tipo_oficio = TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", ""))
    has_nuconta = nuconta_info.get("status") not in (None, "N/A", "not_found")
    saldo_infimo = saldo_float < THRESHOLD_INFIMO

    blocks_raw = enrichment.get("blocks", [])
    has_active_judicial_blocks = any(
        b.get("status") not in ("dismissed", None) for b in blocks_raw
    )
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
    saldo_combinado_infimo = saldo_combinado < THRESHOLD_INFIMO

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
            macro_hint = f"Macro T1 — conta zerada, transferência inviável (saldo R$ {saldo_combinado:.2f})"
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
                macro_hint = f"Macro 3 — saldo ínfimo (R$ {saldo_combinado:.2f} < R$ {THRESHOLD_INFIMO:.2f})"
            elif has_active_judicial_blocks:
                macro_hint = f"Macro 5 — bloqueio com bloqueios anteriores ativos (saldo R$ {saldo_combinado:.2f})"
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

    max_retries = 3
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
                    {"role": "system", "content": LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.2,
                max_tokens=4096,
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
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"    [LLM] Tentativa {attempt}/{max_retries} falhou, "
                      f"retentando em {wait}s...")
                _time.sleep(wait)

    if last_error is not None:
        trace["llm_macro_aplicada"] = "ERRO_LLM"
        trace["llm_observacoes"] = (
            f"Falhou após {max_retries} tentativas. Último erro: {last_error}"
        )[:500]

    return trace


# ---------------------------------------------------------------------------
# Phase 4 — Google Sheets
# ---------------------------------------------------------------------------

def write_to_sheets(rows: list[list[str]]) -> str | None:
    """Append data rows to the target Google Sheet (creates header if empty)."""
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
    if not existing:
        ws.update(range_name="A1", values=[HEADER_ROW])
        ws.format("A1:AB1", {"textFormat": {"bold": True}})

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


def generate_doc(case: dict, llm_trace: dict) -> str | None:
    """Call the Apps Script to create a filled letter in the Drive folder.

    Returns the Google Doc URL on success, or None on failure.
    """
    if not settings.apps_script_url:
        print("    ⚠ APPS_SCRIPT_URL não configurada, pulando geração de doc.")
        return None

    tipo = TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", ""))
    processo = _sanitize(case.get("numero_processo")) or "DESCONHECIDO"
    doc_name = f"CR-{processo}-{tipo}"

    cpf_raw = _sanitize(case.get("cpf_cnpj"))
    doc_type = "CPF" if len(cpf_raw.replace(".", "").replace("-", "").replace("/", "")) <= 11 else "CNPJ"

    macro_text = llm_trace.get("llm_texto_resposta", "")
    if not macro_text or macro_text == "N/A":
        macro_text = f"Macro: {llm_trace.get('llm_macro_aplicada', 'N/A')}"

    # Post-process: strip repeated header, force lowercase start
    for prefix in [
        "Em atenção ao ofício judicial, informamos que ",
        "Em atenção ao ofício judicial, ",
        "em atenção ao ofício judicial, informamos que ",
        "em atenção ao ofício judicial, ",
    ]:
        if macro_text.lower().startswith(prefix.lower()):
            macro_text = macro_text[len(prefix):]
            break

    if macro_text and macro_text[0].isupper():
        macro_text = macro_text[0].lower() + macro_text[1:]

    macro_text = _fix_ortografia(macro_text)

    vara = _fix_ortografia(_sanitize(case.get("vara_tribunal")))
    orgao = _fix_ortografia(_sanitize(case.get("orgao_nome")))
    nome = _fix_ortografia(_sanitize(case.get("nome_investigado")))

    replacements = {
        "{{data da elaboração deste documento}}": _format_date_pt(),
        "{{número do ofício}}": _sanitize(case.get("numero_oficio")),
        "{{número do processo}}": processo,
        "{{Vara/Seccional}}": vara,
        "{{Órgão (delegacia/tribunal)}}": orgao,
        "{{NOME DO CLIENTE ATINGIDO}}": nome,
        "CPF (CNPJ)": doc_type,
        "{{documento do cliente atingido}}": _format_cpf(cpf_raw),
        "{{macro da operação realizada}}": macro_text,
    }

    payload = {
        "templateId": settings.google_template_doc_id,
        "folderId": settings.google_drive_folder_id,
        "docName": doc_name,
        "subfolderName": processo,
        "replacements": replacements,
    }

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
            print(f"    ✗ Apps Script erro: {result['error']}")
            return None

        doc_url = result.get("docUrl", "")
        print(f"    ✓ Doc criado: {doc_url}")
        return doc_url

    except Exception as e:
        print(f"    ✗ Erro ao gerar doc: {e}")
        return None


# ---------------------------------------------------------------------------
# Slack Notifier
# ---------------------------------------------------------------------------

_ERROR_ACTION_MAP = {
    "CERTS_NAO_CONFIGURADOS": "Executar `nucli` para renovar os certificados mTLS.",
    "ERRO_LLM": "Verificar conectividade com LiteLLM e re-executar o processo.",
    "ERRO_PARSE_JSON": "Resposta da LLM em formato inválido. Revisar prompt ou re-executar.",
    "NAO_GERADO": "Falha na geração do documento. Verificar Apps Script e permissões do Drive.",
}


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
            self._THREAD_TS_FILE.write_text(json.dumps({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "thread_ts": thread_ts,
                "channel": self._channel,
            }))
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
        if existing_ts:
            self._thread_ts = existing_ts
            self._parent_ts = existing_ts
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
        macro_id: str,
        doc_url: str | None,
        lexia_id: str = "",
    ):
        self._case_counter += 1
        doc_link = f"\n:page_facing_up:  <{doc_url}|Abrir Carta-Resposta>" if doc_url else ""

        tipo_emoji = {
            "BLOQUEIO": ":lock:",
            "DESBLOQUEIO": ":unlock:",
            "TRANSFERÊNCIA": ":arrows_counterclockwise:",
        }.get(tipo, ":question:")
        msg = (
            f":white_check_mark:  *Caso {self._case_counter}/{self._total}*  `{lexia_id}`\n"
            f"\n"
            f">  *Processo:* `{processo}`\n"
            f">  {tipo_emoji}  *Tipo:* {tipo}\n"
            f">  *Macro aplicada:* {macro_id}"
            f"{doc_link}"
        )
        self._post(msg)

    def notify_case_error(
        self, processo: str, tipo: str, error_key: str,
        detail: str = "", lexia_id: str = "",
    ):
        self._case_counter += 1
        action = _ERROR_ACTION_MAP.get(error_key, "Investigar logs do pipeline.")
        detail_line = f"\n>  *Detalhe:* {detail}" if detail else ""

        tipo_emoji = {
            "BLOQUEIO": ":lock:",
            "DESBLOQUEIO": ":unlock:",
            "TRANSFERÊNCIA": ":arrows_counterclockwise:",
        }.get(tipo, ":question:")
        msg = (
            f":x:  *Caso {self._case_counter}/{self._total} — FALHA*  `{lexia_id}`\n"
            f"\n"
            f">  *Processo:* `{processo}`\n"
            f">  {tipo_emoji}  *Tipo:* {tipo}\n"
            f">  *Erro:* {error_key}{detail_line}\n"
            f"\n"
            f":warning:  *Ação necessária:* {action}"
        )
        self._post(msg)

    def notify_case_certs_missing(self, processo: str, tipo: str, lexia_id: str = ""):
        self.notify_case_error(
            processo, tipo, "CERTS_NAO_CONFIGURADOS",
            "Certificados mTLS não encontrados no ambiente.",
            lexia_id,
        )

    def finish(
        self,
        succeeded: int,
        errors: int,
        certs_missing: int,
        duration_secs: float,
    ):
        if not self._enabled:
            return

        mins = int(duration_secs // 60)
        secs = int(duration_secs % 60)
        duration_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"

        status_emoji = ":white_check_mark:" if not errors and not certs_missing else ":warning:"
        status_text = "Concluída com sucesso" if not errors and not certs_missing else "Concluída com pendências"

        result_lines = [f"  :white_check_mark:  Sucesso — *{succeeded}*"]
        if errors:
            result_lines.append(f"  :x:  Erros — *{errors}*")
        if certs_missing:
            result_lines.append(f"  :no_entry_sign:  Certs ausentes — *{certs_missing}*")
        result_block = "\n".join(result_lines)

        msg = (
            f"{self._DIVIDER}\n"
            f"\n"
            f"{status_emoji}  *{status_text}*\n"
            f"\n"
            f"*Resultado:*\n"
            f"{result_block}\n"
            f"\n"
            f":stopwatch:  *Duração:* {duration_str}"
        )
        self._post(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import time as _time

    t_start = _time.monotonic()

    limit = int(os.environ.get("LEXIA_LIMIT", "0")) or None
    processes = TARGET_PROCESSES or None

    print("=" * 70)
    print("LexIA CR — Pipeline de Rastreabilidade")
    if processes:
        print(f"Processos: {len(processes)}")
    else:
        print(f"Modo: últimos casos (LIMIT {limit or 'ALL'})")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
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

    # Deduplication — skip already-processed cases
    print("\n[DEDUP] Verificando casos já processados...")
    already_done = fetch_processed_processes()
    skipped_count = 0
    if already_done:
        before = len(cases)
        cases = [c for c in cases if c.get("numero_processo") not in already_done]
        skipped_count = before - len(cases)
        if skipped_count:
            print(f"  ✓ {skipped_count} caso(s) já processado(s) — pulando")
        if not cases:
            print("\n✓ Todos os casos já foram processados anteriormente.")
            sys.exit(0)
    print(f"  → {len(cases)} caso(s) a processar")

    # Count by type for Slack breakdown
    n_bloqueio = sum(
        1 for c in cases
        if TIPO_MAP.get(c.get("tipo_oficio", ""), "") == "BLOQUEIO"
    )
    n_desbloqueio = sum(
        1 for c in cases
        if TIPO_MAP.get(c.get("tipo_oficio", ""), "") == "DESBLOQUEIO"
    )
    n_transferencia = sum(
        1 for c in cases
        if TIPO_MAP.get(c.get("tipo_oficio", ""), "") == "TRANSFERÊNCIA"
    )

    slack.start_thread(
        total=len(cases),
        bloqueio=n_bloqueio,
        desbloqueio=n_desbloqueio,
        transferencia=n_transferencia,
        skipped=skipped_count,
    )

    all_rows = []

    for i, case in enumerate(cases, 1):
        processo = case.get("numero_processo", "?")
        tipo = TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", ""))
        lexia_id = generate_lexia_id()
        print(f"\n{'—'*70}")
        print(f"[{i}/{len(cases)}] {lexia_id} | Processo: {processo} ({tipo})")
        print(f"  Investigado: {case.get('nome_investigado', 'N/A')}")
        print(f"  CPF/CNPJ: {case.get('cpf_cnpj', 'N/A')}")

        # Phase 2
        print(f"\n  [FASE 2] APIs Nubank — enriquecimento...")
        enrichment = enrich_case(case)
        print(f"    Waze shard:    {enrichment['waze_shard']}")
        print(f"    Customer ID:   {enrichment['customers_customer_id']}")
        print(f"    NuConta:       {enrichment['nuconta_status'][:80]}")
        print(f"    Saldo NuConta: {enrichment['nuconta_saldo'][:80]}")
        print(f"    Crebito:       {enrichment['crebito_cartoes'][:80]}")
        print(f"    Rayquaza:      {enrichment['rayquaza_saldo'][:80]}")
        print(f"    Petrificus:    {enrichment['petrificus_bloqueios'][:80]}")
        print(f"    Mario-Box:     {enrichment['mario_box_caixinhas'][:80]}")
        print(f"    Dados Banc.:   {enrichment['dados_bancarios'][:80]}")

        # Phase 3
        print(f"\n  [FASE 3] LLM — decisão da IA...")
        llm_trace = get_llm_decision(case, enrichment)
        print(f"    Macro:         {llm_trace['llm_macro_aplicada']}")
        print(f"    ID Macro:      {llm_trace['llm_id_macro']}")
        print(f"    Observações:   {llm_trace['llm_observacoes'][:100]}")

        # Phase 5 — Generate Google Doc
        print(f"\n  [FASE 5] Google Drive — gerando carta-resposta...")
        doc_url = generate_doc(case, llm_trace)

        status = "success"
        if enrichment["waze_shard"] == "CERTS_NAO_CONFIGURADOS":
            status = "certs_missing"
        if llm_trace["llm_macro_aplicada"].startswith("ERRO"):
            status = "error"
        if not doc_url and status == "success":
            status = "error"

        # Slack per-case notification
        if status == "success":
            slack.notify_case_success(
                processo, tipo, llm_trace["llm_id_macro"], doc_url, lexia_id,
            )
        elif status == "certs_missing":
            slack.notify_case_certs_missing(processo, tipo, lexia_id)
        else:
            error_key = "ERRO_LLM"
            if llm_trace["llm_macro_aplicada"].startswith("ERRO_PARSE"):
                error_key = "ERRO_PARSE_JSON"
            elif not doc_url:
                error_key = "NAO_GERADO"
            slack.notify_case_error(
                processo, tipo, error_key,
                llm_trace.get("llm_observacoes", "")[:200],
                lexia_id,
            )

        info_sol = case.get("info_solicitada", "")
        if isinstance(info_sol, list):
            info_sol = ", ".join(str(i) for i in info_sol)
        row = [
            lexia_id,
            str(case.get("numero_processo", "")),
            tipo,
            str(info_sol),
            str(case.get("numero_oficio", "")),
            str(case.get("nome_investigado", "")),
            str(case.get("cpf_cnpj", "")),
            str(case.get("vara_tribunal", "")),
            str(case.get("orgao_nome", "")),
            str(case.get("valor_solicitado", "")),
            str(case.get("is_cliente_nu", "")),
            str(case.get("data_recebimento", "")),
            str(enrichment["waze_shard"]),
            str(enrichment["customers_customer_id"]),
            str(enrichment["nuconta_status"]),
            str(enrichment["nuconta_saldo"]),
            str(enrichment["mario_box_caixinhas"]),
            str(enrichment["crebito_cartoes"]),
            str(enrichment["rayquaza_saldo"]),
            str(enrichment["petrificus_bloqueios"]),
            str(enrichment["dados_bancarios"]),
            str(llm_trace["llm_macro_aplicada"]),
            str(llm_trace["llm_id_macro"]),
            str(llm_trace["llm_texto_resposta"]),
            str(llm_trace["llm_observacoes"]),
            str(llm_trace["llm_raw_response"])[:2000],
            doc_url or "NAO_GERADO",
            status,
            datetime.now(timezone.utc).isoformat(),
        ]
        all_rows.append(row)

    # Phase 4
    try:
        write_to_sheets(all_rows)
    except Exception as e:
        print(f"\n[FASE 4] ⚠ Erro ao gravar no Sheets (não impede finalização): {e}")

    print(f"\n{'='*70}")
    print(f"✓ Pipeline concluído — {len(all_rows)} casos processados")
    succeeded = sum(1 for r in all_rows if r[-2] == "success")
    certs_missing = sum(1 for r in all_rows if r[-2] == "certs_missing")
    errors = sum(1 for r in all_rows if r[-2] == "error")
    print(f"  Success: {succeeded} | Certs missing: {certs_missing} | Errors: {errors}")
    print("=" * 70)

    duration = _time.monotonic() - t_start
    slack.finish(succeeded, errors, certs_missing, duration)


if __name__ == "__main__":
    main()
