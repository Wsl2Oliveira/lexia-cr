"""Main orchestrator — ties all modules together into a single pipeline."""
from __future__ import annotations

import asyncio
from dataclasses import asdict

import structlog

from lexia.apis.crebito import find_active_cards
from lexia.apis.customers import find_customer_id
from lexia.apis.petrificus import find_blocks
from lexia.apis.rayquaza import find_available_assets
from lexia.apis.waze import find_shard
from lexia.databricks.query import JudicialCase, fetch_pending_cases
from lexia.docs.generator import generate_letter
from lexia.gemini.prompt import decide_macro

log = structlog.get_logger(__name__)


async def enrich_case(case: JudicialCase) -> dict:
    """Enrich a judicial case with data from internal APIs.

    Steps:
      1. Find shard via Waze (CPF → shard)
      2. Find customer ID (if Nubank customer)
      3. Fetch cards, assets, and blocks in parallel

    Returns a dict with all enriched data.
    """
    cpf = case.cpf_cnpj
    if not cpf:
        log.warning("no_cpf", case_id=case.id)
        return {"case": asdict(case), "cards": [], "assets": [], "blocks": [], "shard": None}

    shard = await find_shard(cpf)
    if not shard:
        log.warning("shard_not_found", case_id=case.id)
        return {"case": asdict(case), "cards": [], "assets": [], "blocks": [], "shard": None}

    customer_id = case.customer_id
    if not customer_id:
        customer_id = await find_customer_id(cpf, shard)

    cards, assets, blocks = [], [], []
    if customer_id:
        cards, assets, blocks = await asyncio.gather(
            find_active_cards(customer_id, shard),
            find_available_assets(customer_id, shard),
            find_blocks(customer_id, shard),
        )

    return {
        "case": asdict(case),
        "cards": cards,
        "assets": assets,
        "blocks": blocks,
        "shard": shard,
        "customer_id": customer_id,
    }


def _format_document(raw: str | None) -> str:
    """Format CPF (000.000.000-00) or CNPJ (00.000.000/0000-00)."""
    digits = (raw or "").replace(".", "").replace("-", "").replace("/", "").replace(" ", "")
    if len(digits) == 11:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    return raw or ""


def _detect_doc_type(raw: str | None) -> str:
    """Return 'CPF' if 11 digits, 'CNPJ' otherwise."""
    digits = (raw or "").replace(".", "").replace("-", "").replace("/", "").replace(" ", "")
    return "CPF" if len(digits) <= 11 else "CNPJ"


def _build_replacements(case: JudicialCase, decision) -> dict[str, str]:
    """Map case fields + Gemini decision to Google Docs template placeholders.

    Keys match the EXACT text in the Google Docs template.
    Bracketed keys (e.g. "número do ofício") are wrapped in {{}} in the doc.
    Plain-text keys (e.g. "CPF (CNPJ)") appear as literal text.
    """
    import locale
    from datetime import date

    try:
        locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    except locale.Error:
        pass

    return {
        "número do ofício": case.numero_oficio or "",
        "número do processo": case.numero_processo or "",
        "NOME DO CLIENTE ATINGIDO": case.nome_investigado or "",
        "documento do cliente atingido": _format_document(case.cpf_cnpj),
        "Vara/Seccional": case.vara_tribunal or "",
        "Órgão (delegacia/tribunal)": case.orgao_nome or "",
        "macro da operação realizada": decision.texto_resposta or "",
        "data da elaboração deste documento": date.today().strftime("%d de %B de %Y"),
        "CPF (CNPJ)": _detect_doc_type(case.cpf_cnpj),
    }


async def process_single_case(case: JudicialCase) -> dict:
    """Process one judicial case end-to-end.

    1. Enrich with API data
    2. Get Gemini macro decision
    3. Generate response letter in Google Docs/Drive

    Returns a result dict with status, doc_url, etc.
    """
    log.info("processing_case", case_id=case.id, tipo=case.tipo_oficio, cpf=case.cpf_cnpj[:3] + "***" if case.cpf_cnpj else "N/A")

    try:
        enriched = await enrich_case(case)

        decision = await decide_macro(
            case_data=enriched["case"],
            cards=enriched["cards"],
            assets=enriched["assets"],
            blocks=enriched["blocks"],
        )

        replacements = _build_replacements(case, decision)

        doc_name = f"CR-{case.numero_oficio or case.id[:8]}-{case.tipo_oficio}"
        letter = generate_letter(doc_name, replacements, export_as_pdf=True)

        result = {
            "case_id": case.id,
            "status": "success",
            "macro": decision.id_macro,
            "macro_name": decision.macro_aplicada,
            **letter,
        }
        log.info("case_completed", **{k: v for k, v in result.items() if k != "texto_resposta"})
        return result

    except Exception as exc:
        log.exception("case_failed", case_id=case.id, error=str(exc))
        return {"case_id": case.id, "status": "error", "error": str(exc)}


async def run_pipeline(days_back: int | None = None, dry_run: bool = False) -> list[dict]:
    """Run the full LexIA pipeline.

    1. Fetch pending cases from Databricks
    2. Process each case sequentially
    3. Return list of results

    Args:
        days_back: Override default days lookback.
        dry_run: If True, fetch cases but don't process.
    """
    cases = fetch_pending_cases(days_back)
    log.info("pipeline_start", total_cases=len(cases), dry_run=dry_run)

    if dry_run:
        return [{"case_id": c.id, "tipo": c.tipo_oficio, "cpf": c.cpf_cnpj, "status": "dry_run"} for c in cases]

    results = []
    for case in cases:
        result = await process_single_case(case)
        results.append(result)

    succeeded = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "error")
    log.info("pipeline_complete", total=len(results), succeeded=succeeded, failed=failed)

    return results
