"""Drop-in replacement for ``get_llm_decision``.

Returns a dict with the same keys the LLM trace produces, so it can be
plugged into the pipeline without changing downstream code (Sheets row
assembly, doc generation, Slack notifications).

Extra keys added (for shadow/hybrid bookkeeping):
    - ``decision_source``: ``"deterministic"``.
    - ``confidence``: ``"HIGH"`` / ``"LOW"``.
    - ``decision_reason``: short human trace from the classifier.
"""

from __future__ import annotations

from typing import Any

from lexia.deterministic.classifier import classify
from lexia.deterministic.templates import render


def decide(case: dict, enrichment: dict) -> dict[str, Any]:
    """Classify + render in one call.

    Returns a dict with the same shape as ``get_llm_decision``'s ``trace``::

        {
            "llm_macro_aplicada": str,
            "llm_id_macro": str,
            "llm_texto_resposta": str,
            "llm_observacoes": str,
            "llm_raw_response": str,
            "decision_source": "deterministic",
            "confidence": "HIGH" | "LOW",
            "decision_reason": str,
        }

    The ``llm_*`` prefix is preserved on purpose so that:
      - the existing Sheet row assembly works unchanged;
      - the Apps Script doc generator (``build_generate_doc_replacements``)
        consumes ``llm_texto_resposta`` and ``llm_id_macro`` transparently.
    """
    decision = classify(case, enrichment)

    trace: dict[str, Any] = {
        "llm_macro_aplicada": decision.macro_name or "DESCONHECIDA",
        "llm_id_macro": decision.macro_id or "0",
        "llm_texto_resposta": "",
        "llm_observacoes": "",
        "llm_raw_response": f"[deterministic] {decision.reason}"[:2000],
        "decision_source": "deterministic",
        "confidence": decision.confidence.value,
        "decision_reason": decision.reason,
    }

    if not decision.macro_id:
        trace["llm_macro_aplicada"] = "ERRO_DETERMINISTIC_NO_MACRO"
        trace["llm_observacoes"] = decision.reason
        return trace

    try:
        texto = render(decision)
        trace["llm_texto_resposta"] = texto[:2000]
    except Exception as e:
        trace["llm_macro_aplicada"] = "ERRO_DETERMINISTIC_RENDER"
        trace["llm_observacoes"] = f"Falha ao renderizar macro {decision.macro_id}: {e}"
        trace["llm_texto_resposta"] = ""

    return trace
