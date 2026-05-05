"""Deterministic decision engine for LexIA macro classification and rendering.

This package replaces the LLM-based decision step with pure-Python rules,
keeping the LLM as an optional fallback for low-confidence cases.

Public API:
    - ``decide(case, enrichment)``: drop-in replacement for ``get_llm_decision``.
    - ``classify(case, enrichment)``: returns the typed classification result.
    - ``render(decision)``: returns the final ``texto_resposta`` string.
"""

from __future__ import annotations

from lexia.deterministic.classifier import (
    Confidence,
    DeterministicDecision,
    classify,
)
from lexia.deterministic.engine import decide
from lexia.deterministic.templates import render

__all__ = [
    "Confidence",
    "DeterministicDecision",
    "classify",
    "decide",
    "render",
]
