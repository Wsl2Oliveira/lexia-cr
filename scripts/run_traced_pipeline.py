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
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path

import gspread
import httpx
from google.oauth2.service_account import Credentials
from openai import OpenAI

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
    "numero_processo",
    "tipo_oficio",
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
    "crebito_cartoes",
    "rayquaza_saldo",
    "petrificus_bloqueios",
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

LLM_SYSTEM_PROMPT = """\
Você é um analista regulatório especializado em ordens judiciais da Nubank.
Analise os dados do caso judicial e decida qual macro de resposta aplicar.

Macros de Bloqueio disponíveis:
1. bloqueio_conta_bloqueada — Conta existe, bloqueio realizado com sucesso
2. bloqueio_inexiste_conta — CPF/CNPJ não possui conta ativa no Nubank
3. bloqueio_conta_zerada — Conta existe mas saldo é zero, bloqueio prejudicado
4. bloqueio_saldo_irrisorio_bacenjud — Saldo <= R$10, art. 13 §10 Bacenjud 2.0
5. bloqueio_cnpj_nao_cadastrado — CNPJ não consta no cadastro
6. bloqueio_conta_pagamentos_explicacao — Esclarecer que Nubank é conta de pagamentos
7. bloqueio_judicial_instaurado — Bloqueio judicial ativo na conta
8. bloqueio_sem_portabilidade_salario — Sem portabilidade de salário
9. bloqueio_monitoramento_recebiveis — Sem produto, monitoramento + Teimosinha

Macros de Desbloqueio:
10. desbloqueio_produtos_livres — Todos os bloqueios judiciais foram encerrados \
(dismissed) e a conta/cartão estão ativos para movimentação

REGRAS DE ANÁLISE OBRIGATÓRIAS:
- DIFERENCIE bloqueio JUDICIAL (Petrificus/freeze-orders) de bloqueio COMERCIAL.
  - Petrificus status "dismissed" = bloqueio judicial FOI encerrado.
  - Cartão status "blocked" com detail "late_blocked" = bloqueio COMERCIAL por inadimplência,
    NÃO é bloqueio judicial.
  - Conta status "internal_delinquent" = inadimplência, NÃO é bloqueio judicial.
- Para DESBLOQUEIO (tipo_oficio = DESBLOQUEIO):
  - Se todos os bloqueios judiciais (Petrificus) estão "dismissed", o desbloqueio foi cumprido.
  - MAS se a conta ou cartão estão bloqueados comercialmente (inadimplência), informe que
    o desbloqueio judicial foi cumprido, porém NÃO diga que "os produtos estão livres para
    movimentação" se a conta estiver como "internal_delinquent" ou cartão "blocked/late_blocked".
  - Neste caso, diga: "os bloqueios judiciais referentes ao processo foram devidamente
    encerrados. Informamos que eventuais restrições nos produtos do cliente decorrem de
    questões comerciais internas, não relacionadas à ordem judicial."
- Para BLOQUEIO:
  - Verifique o saldo disponível (Rayquaza available_amount) para decidir a macro.
  - Saldo R$0,00 = macro 3 (conta zerada). Saldo <= R$10 = macro 4 (irrisório).
- Sempre considere o account_status e o status do cartão na sua análise.

REGRAS DE REDAÇÃO OBRIGATÓRIAS:
- O texto_resposta DEVE começar com letra minúscula, pois ele complementa a frase
  "...em atenção ao ofício judicial, informamos que". Exemplo correto:
  "o CPF indicado não possui conta ativa no Nubank."
  Exemplo ERRADO: "Em atenção ao ofício judicial, informamos que o CPF..."
  O texto NÃO deve repetir "Em atenção ao ofício judicial" — isso já consta no template.
- O texto_resposta deve ser conciso e direto, sem saudações nem fechamento.
- Revise a ortografia: nomes de cidades devem ter acentuação correta
  (ex: "SÃO PAULO" e não "SAO PAULO", "MACEIÓ" e não "MACEIO").

Formato de saída OBRIGATÓRIO (JSON):
{
    "macro_aplicada": "Nome da macro",
    "id_macro": "1-10 ou COMBINADA",
    "valor_bloqueio": "valor em R$ ou null",
    "texto_resposta": "Texto que complementa '...informamos que' — inicia com letra minúscula",
    "observacoes": "Observações ou null"
}

Responda APENAS com o JSON.
"""

# ---------------------------------------------------------------------------
# Phase 1 — Databricks
# ---------------------------------------------------------------------------

QUERY_BY_PROCESSES = """
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
    inv.investigated_information__requested_value             AS valor_solicitado,
    inv.investigated_information__customer_id                 AS customer_id,
    inv.investigated_information__is_customer                 AS is_cliente_nu

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

WHERE ext.official_letter_extraction__process_document_number IN ({placeholders})
  AND ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
ORDER BY ext.official_letter_extraction__created_at DESC
"""


def fetch_cases_from_databricks(processes: list[str]) -> list[dict]:
    """Query Databricks for specific process numbers."""
    from databricks import sql as dbsql

    placeholders = ", ".join(f"'{p}'" for p in processes)
    query = QUERY_BY_PROCESSES.format(placeholders=placeholders)

    print(f"\n[FASE 1] Databricks — consultando {len(processes)} processos...")
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
    print(f"  ✓ {len(cases)} registros retornados")
    for c in cases:
        print(f"    • {c.get('numero_processo')} | {TIPO_MAP.get(c.get('tipo_oficio',''), c.get('tipo_oficio',''))} | {c.get('nome_investigado','N/A')}")

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
        "crebito_cartoes": "N/A",
        "rayquaza_saldo": "N/A",
        "petrificus_bloqueios": "N/A",
        "cards": [],
        "assets": [],
        "blocks": [],
    }

    cpf = case.get("cpf_cnpj")
    if not cpf:
        trace["waze_shard"] = "SEM_CPF"
        return trace

    if not _has_nu_auth():
        for k in ["waze_shard", "customers_customer_id", "crebito_cartoes",
                   "rayquaza_saldo", "petrificus_bloqueios"]:
            trace[k] = "CERTS_NAO_CONFIGURADOS"
        return trace

    try:
        client = _get_nu_client()

        # Waze
        waze_resp = client.post(
            "https://prod-global-waze.nubank.com.br/api/mapping/cpf",
            json={"cpf": cpf},
        )
        waze_resp.raise_for_status()
        shard = waze_resp.json().get("shard")
        trace["waze_shard"] = shard or "NOT_FOUND"

        if not shard:
            client.close()
            return trace

        # Customers
        cust_resp = client.post(
            f"https://prod-{shard}-customers.nubank.com.br/api/customers/person/find-by-tax-id",
            json={"tax_id": cpf},
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

        # Rayquaza
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
                trace["rayquaza_saldo"] = json.dumps(
                    [{"type": a.get("type"), "amount": a.get("amount")} for a in assets_list],
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

        client.close()

    except Exception as e:
        for k in ["waze_shard", "customers_customer_id", "crebito_cartoes",
                   "rayquaza_saldo", "petrificus_bloqueios"]:
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

    # Parse Facade data for LLM context
    facade_info = {}
    try:
        facade_info = json.loads(enrichment.get("crebito_cartoes", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    context = json.dumps(
        {
            "dados_caso": {
                "numero_processo": case.get("numero_processo"),
                "tipo_oficio": TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", "")),
                "numero_oficio": case.get("numero_oficio"),
                "nome_investigado": case.get("nome_investigado"),
                "cpf_cnpj": case.get("cpf_cnpj"),
                "vara_tribunal": case.get("vara_tribunal"),
                "orgao_nome": case.get("orgao_nome"),
                "valor_solicitado": str(case.get("valor_solicitado", "")),
                "is_cliente_nu": case.get("is_cliente_nu"),
                "customer_id": case.get("customer_id"),
            },
            "conta_credito": {
                "account_status": facade_info.get("account_status", "N/A"),
                "cartoes": facade_info.get("cards", []),
                "saldos_credito": facade_info.get("balances", {}),
            },
            "ativos_disponiveis": enrichment.get("assets", []),
            "bloqueios_judiciais": enrichment.get("blocks", []),
        },
        ensure_ascii=False,
        indent=2,
    )

    user_message = f"Analise o caso abaixo e decida a macro:\n\n{context}"

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

    except json.JSONDecodeError:
        trace["llm_macro_aplicada"] = "ERRO_PARSE_JSON"
        trace["llm_observacoes"] = "LLM retornou resposta não-JSON"
    except Exception as e:
        trace["llm_macro_aplicada"] = "ERRO_LLM"
        trace["llm_observacoes"] = str(e)[:500]

    return trace


# ---------------------------------------------------------------------------
# Phase 4 — Google Sheets
# ---------------------------------------------------------------------------

def write_to_sheets(rows: list[list[str]]):
    """Write header + data rows to the target Google Sheet."""
    sa_path = settings.google_service_account_path
    if not sa_path or not Path(sa_path).exists():
        print("\n[FASE 4] ⚠ Google Service Account não configurado, pulando escrita.")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)

    print(f"\n[FASE 4] Google Sheets — escrevendo {len(rows)} linhas...")
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_NAME)

    ws.clear()
    ws.update(range_name="A1", values=[HEADER_ROW] + rows)
    ws.format("A1:W1", {"textFormat": {"bold": True}})

    print(f"  ✓ Planilha atualizada: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")


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
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("LexIA CR — Pipeline de Rastreabilidade")
    print(f"Processos: {len(TARGET_PROCESSES)}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 70)

    # Phase 1
    cases = fetch_cases_from_databricks(TARGET_PROCESSES)
    if not cases:
        print("\n❌ Nenhum caso encontrado no Databricks. Verifique os números de processo.")
        sys.exit(1)

    all_rows = []

    for i, case in enumerate(cases, 1):
        processo = case.get("numero_processo", "?")
        tipo = TIPO_MAP.get(case.get("tipo_oficio", ""), case.get("tipo_oficio", ""))
        print(f"\n{'—'*70}")
        print(f"[{i}/{len(cases)}] Processo: {processo} ({tipo})")
        print(f"  Investigado: {case.get('nome_investigado', 'N/A')}")
        print(f"  CPF/CNPJ: {case.get('cpf_cnpj', 'N/A')}")

        # Phase 2
        print(f"\n  [FASE 2] APIs Nubank — enriquecimento...")
        enrichment = enrich_case(case)
        print(f"    Waze shard:    {enrichment['waze_shard']}")
        print(f"    Customer ID:   {enrichment['customers_customer_id']}")
        print(f"    Crebito:       {enrichment['crebito_cartoes'][:80]}")
        print(f"    Rayquaza:      {enrichment['rayquaza_saldo'][:80]}")
        print(f"    Petrificus:    {enrichment['petrificus_bloqueios'][:80]}")

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

        row = [
            str(case.get("numero_processo", "")),
            tipo,
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
            str(enrichment["crebito_cartoes"]),
            str(enrichment["rayquaza_saldo"]),
            str(enrichment["petrificus_bloqueios"]),
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
    write_to_sheets(all_rows)

    print(f"\n{'='*70}")
    print(f"✓ Pipeline concluído — {len(all_rows)} casos processados")
    succeeded = sum(1 for r in all_rows if r[-2] == "success")
    certs_missing = sum(1 for r in all_rows if r[-2] == "certs_missing")
    errors = sum(1 for r in all_rows if r[-2] == "error")
    print(f"  Success: {succeeded} | Certs missing: {certs_missing} | Errors: {errors}")
    print("=" * 70)


if __name__ == "__main__":
    main()
