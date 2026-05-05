"""Deterministic macro classifier for LexIA.

Extracts the rule logic from ``scripts/run_traced_pipeline.py::get_llm_decision``
into a pure function returning a typed result. No I/O, no LLM calls.

The mapping mirrors exactly the rules already encoded in the LLM prompt's
``analise_pre_calculada.macro_sugerida`` hint, which today the LLM is
instructed to follow verbatim.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

THRESHOLD_INFIMO = 10.0

TIPO_MAP = {
    "official_letter_type__block": "BLOQUEIO",
    "official_letter_type__dismiss": "DESBLOQUEIO",
    "official_letter_type__transfer": "TRANSFERÊNCIA",
}


class Confidence(str, Enum):
    """Confidence in the deterministic classification.

    HIGH: rules unambiguously selected a macro; safe to skip the LLM.
    LOW: ambiguity detected (unknown ``tipo_oficio``, missing data, etc.);
         caller may fall back to the LLM.
    """

    HIGH = "HIGH"
    LOW = "LOW"


@dataclass
class DeterministicDecision:
    """Typed output of :func:`classify`."""

    macro_id: str
    """Macro identifier: ``"1"``, ``"1B"``, ``"2"``..``"9"``, ``"T1"``, ``"T2"``, ``"T3"`` or ``""`` if undecided."""

    macro_name: str
    """Human-readable macro label (matches ``llm_macro_aplicada`` field)."""

    confidence: Confidence
    """Whether the rule is firm enough to bypass LLM fallback."""

    tipo_oficio: str
    """Normalized: ``"BLOQUEIO"``, ``"DESBLOQUEIO"``, ``"TRANSFERÊNCIA"`` or ``""``."""

    saldo_combinado: float = 0.0
    """NuConta available + caixinhas (used for Macros 3/4/5/6/T1/T3)."""

    saldo_nuconta: float = 0.0
    saldo_caixinhas: float = 0.0
    frozen_amount: float = 0.0
    """Sum of judicial blocks with status='frozen' (used for Macro 1B)."""

    has_nuconta: bool = False
    has_credit_account: bool = False
    has_cards: bool = False
    has_active_judicial_blocks: bool = False
    is_nao_cliente: bool = False

    info_solicitada: list[str] = field(default_factory=list)
    """Normalized list of requested information items."""

    dados_bancarios: dict[str, Any] = field(default_factory=dict)
    """{"agencia": "...", "conta": "...", "banco": "..."}."""

    requires_dados_bancarios: bool = False
    """True if info_solicitada contains 'Dados Bancarios'/'Bancários'."""

    requires_restricao_comercial: bool = False
    """True for Macro 1 (DESBLOQUEIO) when the account is commercially blocked."""

    cpf_cnpj_raw: str = ""
    doc_type: str = "CPF"
    """``"CPF"`` if ≤11 digits, else ``"CNPJ"``."""

    customer_id: str = ""

    saldos_credito: dict[str, Any] = field(default_factory=dict)
    """Credit card balances dict (used for Macro T3 fatura calculation)."""

    assets: list[Any] = field(default_factory=list)
    """Investment assets (used for Macro T3 detail block)."""

    reason: str = ""
    """Short human-readable trace explaining the rule that fired."""


def _safe_json_loads(raw: Any) -> dict | list:
    if isinstance(raw, (dict, list)):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def _normalize_info_solicitada(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    if isinstance(raw, str):
        if not raw.strip():
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
            return [str(parsed)] if parsed else []
        except (json.JSONDecodeError, TypeError):
            return [raw]
    return []


def _normalize_doc_type(cpf_cnpj: str) -> str:
    digits = (cpf_cnpj or "").replace(".", "").replace("-", "").replace("/", "").strip()
    return "CPF" if len(digits) <= 11 else "CNPJ"


# Campos do enrichment cuja falha invalida a decisão determinística:
# sem shard (Waze), sem customer_id (Customers) ou sem visão de crédito
# (Facade) o classifier acaba decidindo com base em dados falsamente
# vazios — tipicamente "Macro 2 / Macro 3" para casos que na verdade têm
# saldo. Quando qualquer um desses traz "ERRO:" no início, devolvemos
# LOW para que o modo hybrid faça fallback automático para o LLM.
_CRITICAL_ENRICHMENT_FIELDS = (
    "waze_shard",
    "customers_customer_id",
    "crebito_cartoes",
)


def _detect_enrichment_failure(enrichment: dict) -> str:
    """Return a human-readable reason if a critical API call failed, else ""."""
    failed = []
    for key in _CRITICAL_ENRICHMENT_FIELDS:
        value = str(enrichment.get(key, "") or "")
        if value.startswith("ERRO:"):
            failed.append(f"{key}={value[:80]}")
    if not failed:
        return ""
    return "Enriquecimento crítico falhou: " + " | ".join(failed)


def _detect_restricao_comercial(nuconta_info: dict, facade_info: dict) -> bool:
    """Account is bloqueada comercialmente (inadimplência), not judicially.

    Mirrors the LLM prompt rule: ``internal_delinquent`` / commercial-blocked
    cards trigger an extra warning paragraph in Macro 1.
    """
    nuconta_status = (nuconta_info.get("status") or "").lower()
    if "delinquent" in nuconta_status or "internal_delinquent" in nuconta_status:
        return True
    account_status = (facade_info.get("account_status") or "").lower()
    if "delinquent" in account_status:
        return True
    cards = facade_info.get("cards") or []
    for card in cards:
        if not isinstance(card, dict):
            continue
        status = (card.get("status") or "").lower()
        flags = card.get("flags") or card.get("status_flags") or []
        flags_str = " ".join(str(f).lower() for f in flags) if isinstance(flags, list) else str(flags).lower()
        if "late_blocked" in status or "late_blocked" in flags_str:
            return True
    return False


def classify(case: dict, enrichment: dict) -> DeterministicDecision:
    """Classify a single investigated row into a macro.

    Args:
        case: Raw case dict from Databricks (one investigated row).
        enrichment: Output of ``enrich_case`` (Nubank API enrichment).

    Returns:
        :class:`DeterministicDecision` with the chosen ``macro_id`` and all
        the contextual data needed by :func:`lexia.deterministic.templates.render`.
    """

    facade_info = _safe_json_loads(enrichment.get("crebito_cartoes", "{}"))
    nuconta_info = _safe_json_loads(enrichment.get("nuconta_status", "{}"))
    nuconta_saldo = _safe_json_loads(enrichment.get("nuconta_saldo", "{}"))
    rayquaza_info = _safe_json_loads(enrichment.get("rayquaza_saldo", "{}"))
    dados_bancarios = _safe_json_loads(enrichment.get("dados_bancarios", "{}"))

    if not isinstance(facade_info, dict):
        facade_info = {}
    if not isinstance(nuconta_info, dict):
        nuconta_info = {}
    if not isinstance(nuconta_saldo, dict):
        nuconta_saldo = {}
    if not isinstance(rayquaza_info, dict):
        rayquaza_info = {}
    if not isinstance(dados_bancarios, dict):
        dados_bancarios = {}

    saldo_nuconta = _safe_float(nuconta_saldo.get("available"))
    saldo_caixinhas = _safe_float(rayquaza_info.get("caixinhas_total"))
    saldo_total_rayquaza = _safe_float(rayquaza_info.get("total_disponivel"))
    saldo_combinado = max(saldo_total_rayquaza, saldo_nuconta + saldo_caixinhas)
    saldo_combinado_infimo = saldo_combinado < THRESHOLD_INFIMO

    has_nuconta = nuconta_info.get("status") not in (None, "N/A", "not_found", "")
    has_credit_account = facade_info.get("account_status") not in (None, "N/A", "not_found", "")
    has_cards = bool(facade_info.get("cards"))

    blocks_raw = enrichment.get("blocks") or []
    if isinstance(blocks_raw, str):
        loaded = _safe_json_loads(blocks_raw)
        blocks_raw = loaded if isinstance(loaded, list) else []
    has_active_judicial_blocks = any(
        (b.get("status") if isinstance(b, dict) else None) not in ("dismissed", None)
        for b in blocks_raw
    )
    frozen_amount = sum(
        _safe_float(b.get("frozen_amount", b.get("amount", 0)))
        for b in blocks_raw
        if isinstance(b, dict) and b.get("status") == "frozen"
    )

    info_solicitada = _normalize_info_solicitada(case.get("info_solicitada"))
    info_lower = [i.lower() for i in info_solicitada]

    requires_dados_bancarios = any(
        ("dados" in i and ("bancario" in i or "bancário" in i)) for i in info_lower
    )

    customer_id_val = str(enrichment.get("customers_customer_id", "") or "")
    is_nao_cliente = customer_id_val in ("NAO_CLIENTE", "NOT_FOUND")

    cpf_cnpj_raw = str(case.get("cpf_cnpj") or "")
    doc_type = _normalize_doc_type(cpf_cnpj_raw)

    tipo_raw = case.get("tipo_oficio", "") or ""
    tipo_oficio = TIPO_MAP.get(tipo_raw, tipo_raw)

    decision = DeterministicDecision(
        macro_id="",
        macro_name="",
        confidence=Confidence.LOW,
        tipo_oficio=tipo_oficio,
        saldo_combinado=saldo_combinado,
        saldo_nuconta=saldo_nuconta,
        saldo_caixinhas=saldo_caixinhas,
        frozen_amount=frozen_amount,
        has_nuconta=has_nuconta,
        has_credit_account=has_credit_account,
        has_cards=has_cards,
        has_active_judicial_blocks=has_active_judicial_blocks,
        is_nao_cliente=is_nao_cliente,
        info_solicitada=info_solicitada,
        dados_bancarios=dados_bancarios,
        requires_dados_bancarios=requires_dados_bancarios,
        cpf_cnpj_raw=cpf_cnpj_raw,
        doc_type=doc_type,
        customer_id=customer_id_val,
        saldos_credito=facade_info.get("balances") or {},
        assets=enrichment.get("assets") or [],
    )

    enrichment_failure = _detect_enrichment_failure(enrichment)
    if enrichment_failure:
        decision.macro_id = ""
        decision.macro_name = "ENRICHMENT_FAILED"
        decision.confidence = Confidence.LOW
        decision.reason = enrichment_failure
        return decision

    if tipo_oficio == "DESBLOQUEIO":
        is_desbloqueio_conta = any("desbloqueio" in i and "conta" in i for i in info_lower)
        is_desbloqueio_valores = any("desbloqueio" in i and "valor" in i for i in info_lower)

        if is_desbloqueio_conta and not is_desbloqueio_valores and frozen_amount > 0:
            decision.macro_id = "1B"
            decision.macro_name = "DESBLOQUEIO DE CONTA COM VALORES CONSTRITOS"
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"DESBLOQUEIO de conta + frozen_amount=R$ {frozen_amount:.2f} > 0"
            )
        else:
            decision.macro_id = "1"
            decision.macro_name = "DESBLOQUEIO REALIZADO"
            decision.confidence = Confidence.HIGH
            decision.reason = "DESBLOQUEIO sem valores constritos ativos"
            decision.requires_restricao_comercial = _detect_restricao_comercial(
                nuconta_info, facade_info
            )
        return decision

    if tipo_oficio == "TRANSFERÊNCIA":
        if is_nao_cliente:
            decision.macro_id = "T2"
            decision.macro_name = "NÃO É CLIENTE (TRANSFERÊNCIA)"
            decision.confidence = Confidence.HIGH
            decision.reason = f"TRANSFERÊNCIA + customer_id={customer_id_val}"
        elif saldo_combinado_infimo:
            decision.macro_id = "T1"
            decision.macro_name = "CONTA ZERADA (TRANSFERÊNCIA INVIÁVEL)"
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"TRANSFERÊNCIA + saldo_combinado=R$ {saldo_combinado:.2f} < "
                f"R$ {THRESHOLD_INFIMO:.2f}"
            )
        else:
            decision.macro_id = "T3"
            decision.macro_name = "TRANSFERÊNCIA REALIZADA"
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"TRANSFERÊNCIA + cliente + saldo_combinado=R$ {saldo_combinado:.2f}"
            )
        return decision

    if tipo_oficio == "BLOQUEIO":
        is_cartao_request = any("cartão" in i or "cartao" in i for i in info_lower)
        is_valores_request = any("valores" in i for i in info_lower)
        is_conta_request = any("conta" in i and "bloqueio" in i for i in info_lower)

        if is_cartao_request and not is_valores_request:
            if has_cards:
                decision.macro_id = "8"
                decision.macro_name = "BLOQUEIO DE CARTÃO DE CRÉDITO"
                decision.confidence = Confidence.HIGH
                decision.reason = "BLOQUEIO de cartão (info_solicitada) + has_cards=True"
            else:
                decision.macro_id = "9"
                decision.macro_name = "NEGATIVA BLOQUEIO DE CARTÃO DE CRÉDITO"
                decision.confidence = Confidence.HIGH
                decision.reason = "BLOQUEIO de cartão (info_solicitada) + has_cards=False"
            return decision

        if is_conta_request and not is_valores_request and not is_cartao_request:
            decision.macro_id = "6"
            decision.macro_name = "BLOQUEIO TOTAL DA CONTA DE PAGAMENTO"
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"BLOQUEIO de conta + saldo_combinado=R$ {saldo_combinado:.2f}"
            )
            return decision

        if not has_nuconta and not has_credit_account:
            if has_cards:
                decision.macro_id = "7"
                decision.macro_name = "CLIENTE NÃO TEM CONTA DO NUBANK, SÓ TEM CARTÃO DE CRÉDITO"
                decision.confidence = Confidence.HIGH
                decision.reason = "BLOQUEIO sem nuconta sem credito mas com cartões"
            else:
                decision.macro_id = "2"
                decision.macro_name = "NÃO POSSUI CONTA ATIVA"
                decision.confidence = Confidence.HIGH
                decision.reason = "BLOQUEIO sem nuconta + sem conta credito + sem cartões"
            return decision

        if saldo_combinado_infimo:
            decision.macro_id = "3"
            decision.macro_name = "SALDO ZERADO OU VALOR ÍNFIMO"
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"BLOQUEIO + saldo_combinado=R$ {saldo_combinado:.2f} < "
                f"R$ {THRESHOLD_INFIMO:.2f}"
            )
            return decision

        if has_active_judicial_blocks:
            decision.macro_id = "5"
            decision.macro_name = (
                "EXISTÊNCIA DE BLOQUEIO ANTERIOR. BLOQUEIO DE VALOR IGUAL AO DETERMINADO."
            )
            decision.confidence = Confidence.HIGH
            decision.reason = (
                f"BLOQUEIO + saldo_combinado=R$ {saldo_combinado:.2f} >= R$ "
                f"{THRESHOLD_INFIMO:.2f} + bloqueio judicial anterior ativo"
            )
            return decision

        decision.macro_id = "4"
        decision.macro_name = "BLOQUEIO DE VALOR IGUAL AO DETERMINADO"
        decision.confidence = Confidence.HIGH
        decision.reason = (
            f"BLOQUEIO + saldo_combinado=R$ {saldo_combinado:.2f} >= R$ "
            f"{THRESHOLD_INFIMO:.2f} + sem bloqueio anterior"
        )
        return decision

    decision.macro_id = ""
    decision.macro_name = "DESCONHECIDA"
    decision.confidence = Confidence.LOW
    decision.reason = (
        f"tipo_oficio inesperado: {tipo_raw!r}. Sem regra deterministica aplicavel."
    )
    return decision
