"""Run-level observability primitives for the LexIA pipeline.

This module groups three concerns that together implement the AC of
JUD-1995 (log-based monitoring of LexIA CR responsiveness):

* :class:`ErrorCategory` — canonical taxonomy used in logs, Slack
  notifications and the spreadsheet ``error_category`` column.
* :class:`RunSummary` — accumulator filled while the pipeline iterates
  over ofícios; produces per-category counts, per-case durations and the
  payload used by :func:`compute_slo_report`.
* :class:`SLOTargets` / :func:`compute_slo_report` — definition of the
  three responsiveness SLOs agreed with Ops Excellence and the helper
  that turns a :class:`RunSummary` into a :class:`SLOReport` ready to
  ship to Slack and to ``logs/run-summary-{YYYY-MM-DD}.json``.

No I/O happens here: callers (``scripts/run_traced_pipeline.py``,
``SlackNotifier``) wire this module into the pipeline.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class ErrorCategory(str, Enum):
    """Canonical reasons for a case ending in non-success status.

    The values are also written verbatim to the spreadsheet
    ``error_category`` column and to ``logs/run-summary-*.json`` so that
    Slack messages, logs and the historical record stay in sync.
    """

    NONE = "NONE"
    """No failure — case completed successfully."""

    CERTS_MISSING = "CERTS_MISSING"
    """mTLS certificates not configured / expired."""

    DATABRICKS = "DATABRICKS"
    """Failure fetching cases from Databricks."""

    ENRICHMENT_API = "ENRICHMENT_API"
    """One of the critical Nu APIs (Waze / Customers / Facade) failed."""

    LLM_RATE_LIMIT = "LLM_RATE_LIMIT"
    """LLM provider returned 429 / quota exhausted."""

    LLM_TRANSIENT = "LLM_TRANSIENT"
    """Generic LLM failure (timeout, 5xx, retry budget exhausted)."""

    LLM_PARSE = "LLM_PARSE"
    """LLM responded but the JSON was malformed."""

    DOC_GENERATION = "DOC_GENERATION"
    """Apps Script / Drive failed to materialize the Google Doc."""

    SHEETS = "SHEETS"
    """Spreadsheet write failed — case ran fine but log row not stored."""

    UNKNOWN = "UNKNOWN"
    """Anything that did not match a more specific category."""


# Mapping consumed by SlackNotifier to render the "Ação necessária" line.
# Keep the keys in sync with the enum above.
ERROR_ACTIONS: dict[ErrorCategory, str] = {
    ErrorCategory.CERTS_MISSING: "Renovar certificados mTLS com `nucli setup` e re-executar.",
    ErrorCategory.DATABRICKS: "Verificar conectividade com o Databricks e a query em `notebooks/lexia_casos_pendentes.sql`.",
    ErrorCategory.ENRICHMENT_API: "API interna do Nubank degradada. Conferir Waze/Customers/Facade e reprocessar caso.",
    ErrorCategory.LLM_RATE_LIMIT: "Rate limit no LiteLLM. Aguardar a janela ou trocar `LITELLM_MODEL` e reprocessar.",
    ErrorCategory.LLM_TRANSIENT: "Falha pontual da LLM. Reprocessar o caso pela planilha (status=`reprocessar`).",
    ErrorCategory.LLM_PARSE: "Resposta da LLM em formato inválido. Revisar prompt ou reprocessar.",
    ErrorCategory.DOC_GENERATION: "Falha na geração do Google Doc. Verificar Apps Script e permissões do Drive.",
    ErrorCategory.SHEETS: "Caso processado, mas planilha não atualizou. Verificar Apps Script de logs.",
    ErrorCategory.UNKNOWN: "Investigar logs do pipeline para identificar a causa.",
}


# ---------------------------------------------------------------------------
# Categorization helper
# ---------------------------------------------------------------------------

# Critical APIs whose failure marks the entire case as ENRICHMENT_API.
# Mirrors src/lexia/deterministic/classifier._CRITICAL_ENRICHMENT_FIELDS.
_CRITICAL_API_FIELDS = ("waze_shard", "customers_customer_id", "crebito_cartoes")


def categorize_case_error(
    *,
    enrichments: list[dict] | None = None,
    inv_results: list[dict] | None = None,
    doc_url: str | None = None,
    overall_status: str = "success",
) -> ErrorCategory:
    """Inspect the artifacts of a single ofício and return its category.

    The function is deliberately conservative: it returns the most
    specific category for which we have a strong signal. Unknown
    failures fall through to :attr:`ErrorCategory.UNKNOWN`.

    Args:
        enrichments: List of enrichment dicts (one per investigado).
        inv_results: List of ``{"llm_trace": ..., ...}`` dicts.
        doc_url: URL of the generated Google Doc, ``None`` if not generated.
        overall_status: Aggregate status decided by the pipeline
            (``success`` / ``error`` / ``certs_missing``).
    """
    if overall_status == "success":
        return ErrorCategory.NONE

    if overall_status == "certs_missing":
        return ErrorCategory.CERTS_MISSING

    enrichments = enrichments or []
    inv_results = inv_results or []

    for enr in enrichments:
        if not isinstance(enr, dict):
            continue
        for key in _CRITICAL_API_FIELDS:
            value = str(enr.get(key, "") or "")
            if value.startswith("ERRO:") or value == "CERTS_NAO_CONFIGURADOS":
                return ErrorCategory.ENRICHMENT_API

    for r in inv_results:
        trace = (r or {}).get("llm_trace") or {}
        macro = str(trace.get("llm_macro_aplicada", "") or "")
        obs = str(trace.get("llm_observacoes", "") or "")
        obs_low = obs.lower()
        if macro.startswith("ERRO_PARSE"):
            return ErrorCategory.LLM_PARSE
        if "ratelimit" in obs_low or "rate limit" in obs_low or "429" in obs_low:
            return ErrorCategory.LLM_RATE_LIMIT
        if macro.startswith("ERRO"):
            return ErrorCategory.LLM_TRANSIENT

    if doc_url is None or doc_url == "NAO_GERADO":
        return ErrorCategory.DOC_GENERATION

    return ErrorCategory.UNKNOWN


# ---------------------------------------------------------------------------
# Run summary
# ---------------------------------------------------------------------------


@dataclass
class CaseRecord:
    """Per-ofício row used to compute aggregates and SLOs."""

    lexia_id: str
    id_oficio: str
    numero_processo: str
    tipo_oficio: str
    status: str
    """``success``, ``error`` or ``certs_missing`` (mirrors planilha)."""
    error_category: ErrorCategory = ErrorCategory.NONE
    duration_secs: float = 0.0
    decision_source: str = ""
    """``deterministic`` / ``llm`` / ``llm_fallback`` / ``shadow`` etc."""


@dataclass
class RunSummary:
    """Accumulator filled by ``main()`` while iterating over ofícios."""

    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: str | None = None
    decision_mode: str = ""
    days_back: int = 0
    skipped_already_processed: int = 0
    cases: list[CaseRecord] = field(default_factory=list)

    # ---- mutation helpers ----

    def add(self, record: CaseRecord) -> None:
        self.cases.append(record)

    def mark_finished(self) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()

    # ---- aggregates ----

    @property
    def total(self) -> int:
        return len(self.cases)

    @property
    def succeeded(self) -> int:
        return sum(1 for c in self.cases if c.status == "success")

    @property
    def errors(self) -> int:
        return sum(1 for c in self.cases if c.status == "error")

    @property
    def certs_missing(self) -> int:
        return sum(1 for c in self.cases if c.status == "certs_missing")

    @property
    def total_duration_secs(self) -> float:
        return sum(c.duration_secs for c in self.cases)

    @property
    def avg_duration_secs(self) -> float:
        if not self.cases:
            return 0.0
        return self.total_duration_secs / self.total

    def percentile_duration_secs(self, p: float) -> float:
        """Return the requested percentile of per-case durations.

        Uses the nearest-rank method (no numpy required).
        """
        if not self.cases:
            return 0.0
        sorted_d = sorted(c.duration_secs for c in self.cases)
        if p <= 0:
            return sorted_d[0]
        if p >= 100:
            return sorted_d[-1]
        rank = int(math.ceil((p / 100.0) * len(sorted_d)))
        return sorted_d[max(0, rank - 1)]

    @property
    def errors_by_category(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for c in self.cases:
            if c.error_category == ErrorCategory.NONE:
                continue
            key = c.error_category.value
            out[key] = out.get(key, 0) + 1
        return out

    @property
    def fallback_llm_count(self) -> int:
        """Cases where the deterministic engine had to fall back to LLM.

        In hybrid mode, ``decision_source`` becomes ``llm_fallback`` when
        the deterministic confidence was LOW (or enrichment failed) and
        the LLM had to step in. This is the metric that drives the
        "fallback rate" SLO.
        """
        return sum(1 for c in self.cases if c.decision_source == "llm_fallback")

    @property
    def error_rate(self) -> float:
        if not self.cases:
            return 0.0
        non_success = self.errors + self.certs_missing
        return non_success / self.total

    @property
    def fallback_rate(self) -> float:
        if not self.cases:
            return 0.0
        return self.fallback_llm_count / self.total

    # ---- serialization ----

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "decision_mode": self.decision_mode,
            "days_back": self.days_back,
            "skipped_already_processed": self.skipped_already_processed,
            "totals": {
                "total": self.total,
                "succeeded": self.succeeded,
                "errors": self.errors,
                "certs_missing": self.certs_missing,
            },
            "errors_by_category": self.errors_by_category,
            "duration_secs": {
                "total": round(self.total_duration_secs, 2),
                "avg": round(self.avg_duration_secs, 2),
                "p50": round(self.percentile_duration_secs(50), 2),
                "p95": round(self.percentile_duration_secs(95), 2),
                "max": round(self.percentile_duration_secs(100), 2),
            },
            "decision": {
                "fallback_llm_count": self.fallback_llm_count,
                "fallback_rate": round(self.fallback_rate, 4),
                "error_rate": round(self.error_rate, 4),
            },
            "cases": [
                {
                    **asdict(c),
                    "error_category": c.error_category.value,
                }
                for c in self.cases
            ],
        }


# ---------------------------------------------------------------------------
# SLO definition + report
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SLOTargets:
    """Responsiveness SLOs agreed with Ops Excellence (JUD-1995).

    These are the production values; tests can override them. Adjust
    here when the team agrees on a new target.
    """

    p95_seconds: float = 60.0
    """A single ofício should take ≤ 60s end-to-end at the 95th percentile."""

    error_rate: float = 0.10
    """No more than 10% of ofícios in a run can finish in non-success status."""

    fallback_rate: float = 0.30
    """No more than 30% of ofícios should require the LLM fallback."""


@dataclass
class SLOViolation:
    name: str
    target: float
    actual: float
    message: str


@dataclass
class SLOReport:
    targets: SLOTargets
    violations: list[SLOViolation]

    @property
    def healthy(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "targets": asdict(self.targets),
            "violations": [asdict(v) for v in self.violations],
        }


def compute_slo_report(summary: RunSummary, targets: SLOTargets | None = None) -> SLOReport:
    """Evaluate a :class:`RunSummary` against the SLOs."""
    targets = targets or SLOTargets()
    violations: list[SLOViolation] = []

    if summary.total > 0:
        p95 = summary.percentile_duration_secs(95)
        if p95 > targets.p95_seconds:
            violations.append(
                SLOViolation(
                    name="p95_seconds",
                    target=targets.p95_seconds,
                    actual=round(p95, 2),
                    message=f"p95 de duração por ofício {p95:.1f}s acima do alvo {targets.p95_seconds:.0f}s",
                )
            )
        if summary.error_rate > targets.error_rate:
            violations.append(
                SLOViolation(
                    name="error_rate",
                    target=targets.error_rate,
                    actual=round(summary.error_rate, 4),
                    message=f"Taxa de erro {summary.error_rate:.1%} acima do alvo {targets.error_rate:.0%}",
                )
            )
        if summary.fallback_rate > targets.fallback_rate:
            violations.append(
                SLOViolation(
                    name="fallback_rate",
                    target=targets.fallback_rate,
                    actual=round(summary.fallback_rate, 4),
                    message=f"Uso de fallback LLM {summary.fallback_rate:.1%} acima do alvo {targets.fallback_rate:.0%}",
                )
            )

    return SLOReport(targets=targets, violations=violations)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def write_run_summary_json(summary: RunSummary, slo: SLOReport, logs_dir: Path) -> Path:
    """Write the run summary + SLO report to ``logs/run-summary-{date}.json``.

    The filename uses the local date so that one file per run-day exists
    for the on-call to inspect. If multiple runs happen in the same day,
    the file is rewritten with the latest summary.
    """
    logs_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = logs_dir / f"run-summary-{date_str}.json"
    payload = {
        "summary": summary.to_dict(),
        "slo": slo.to_dict(),
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return out_path
