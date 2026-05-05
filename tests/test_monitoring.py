"""Tests for the monitoring module (JUD-1995)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lexia.monitoring import (  # noqa: E402, I001
    CaseRecord,
    ErrorCategory,
    RunSummary,
    SLOTargets,
    categorize_case_error,
    compute_slo_report,
)


# ---------------------------------------------------------------------------
# categorize_case_error
# ---------------------------------------------------------------------------


def test_categorize_success_returns_none():
    assert (
        categorize_case_error(overall_status="success")
        == ErrorCategory.NONE
    )


def test_categorize_certs_missing():
    assert (
        categorize_case_error(overall_status="certs_missing")
        == ErrorCategory.CERTS_MISSING
    )


def test_categorize_enrichment_api_failure_via_waze():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[{"waze_shard": "ERRO: timeout 10s"}],
    )
    assert cat == ErrorCategory.ENRICHMENT_API


def test_categorize_enrichment_api_failure_via_customers():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[
            {
                "waze_shard": "shard-12",
                "customers_customer_id": "ERRO: 503",
            }
        ],
    )
    assert cat == ErrorCategory.ENRICHMENT_API


def test_categorize_llm_rate_limit_from_observacoes():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[{"waze_shard": "shard-12"}],
        inv_results=[
            {
                "llm_trace": {
                    "llm_macro_aplicada": "ERRO_LLM",
                    "llm_observacoes": "Falhou: Error code: 429 - litellm.RateLimitErr",
                }
            }
        ],
    )
    assert cat == ErrorCategory.LLM_RATE_LIMIT


def test_categorize_llm_parse():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[{"waze_shard": "shard-12"}],
        inv_results=[
            {
                "llm_trace": {
                    "llm_macro_aplicada": "ERRO_PARSE_JSON",
                    "llm_observacoes": "json malformado",
                }
            }
        ],
    )
    assert cat == ErrorCategory.LLM_PARSE


def test_categorize_doc_generation_when_no_doc_url():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[{"waze_shard": "shard-12"}],
        inv_results=[
            {
                "llm_trace": {
                    "llm_macro_aplicada": "MACRO 4",
                    "llm_observacoes": "ok",
                }
            }
        ],
        doc_url=None,
    )
    assert cat == ErrorCategory.DOC_GENERATION


def test_categorize_unknown_when_no_signal_matches():
    cat = categorize_case_error(
        overall_status="error",
        enrichments=[{"waze_shard": "shard-12"}],
        inv_results=[
            {
                "llm_trace": {
                    "llm_macro_aplicada": "MACRO 4",
                    "llm_observacoes": "ok",
                }
            }
        ],
        doc_url="https://docs.google.com/abc",
    )
    assert cat == ErrorCategory.UNKNOWN


# ---------------------------------------------------------------------------
# RunSummary
# ---------------------------------------------------------------------------


def _make_summary(durations: list[float], statuses: list[str] | None = None) -> RunSummary:
    statuses = statuses or ["success"] * len(durations)
    summary = RunSummary(decision_mode="hybrid", days_back=7)
    for i, (d, status) in enumerate(zip(durations, statuses)):
        summary.add(
            CaseRecord(
                lexia_id=f"LX-TEST-{i:04d}",
                id_oficio=f"oficio-{i}",
                numero_processo=f"000{i}-00.0000.0.00.0000",
                tipo_oficio="BLOQUEIO",
                status=status,
                duration_secs=d,
            )
        )
    return summary


def test_percentile_duration_nearest_rank():
    s = _make_summary([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    assert s.percentile_duration_secs(50) == 50.0
    assert s.percentile_duration_secs(95) == 100.0
    assert s.percentile_duration_secs(100) == 100.0


def test_percentile_empty_summary():
    s = RunSummary()
    assert s.percentile_duration_secs(95) == 0.0


def test_summary_aggregates_by_status():
    s = _make_summary(
        [10, 20, 30, 40],
        statuses=["success", "success", "error", "certs_missing"],
    )
    assert s.total == 4
    assert s.succeeded == 2
    assert s.errors == 1
    assert s.certs_missing == 1
    assert s.error_rate == 0.5


def test_summary_errors_by_category():
    s = RunSummary()
    s.add(
        CaseRecord(
            lexia_id="LX-1",
            id_oficio="o1",
            numero_processo="p1",
            tipo_oficio="BLOQUEIO",
            status="error",
            error_category=ErrorCategory.LLM_RATE_LIMIT,
            duration_secs=15.0,
        )
    )
    s.add(
        CaseRecord(
            lexia_id="LX-2",
            id_oficio="o2",
            numero_processo="p2",
            tipo_oficio="BLOQUEIO",
            status="error",
            error_category=ErrorCategory.LLM_RATE_LIMIT,
            duration_secs=20.0,
        )
    )
    s.add(
        CaseRecord(
            lexia_id="LX-3",
            id_oficio="o3",
            numero_processo="p3",
            tipo_oficio="BLOQUEIO",
            status="error",
            error_category=ErrorCategory.ENRICHMENT_API,
            duration_secs=30.0,
        )
    )
    assert s.errors_by_category == {
        "LLM_RATE_LIMIT": 2,
        "ENRICHMENT_API": 1,
    }


def test_summary_fallback_rate():
    s = RunSummary()
    for i in range(10):
        s.add(
            CaseRecord(
                lexia_id=f"LX-{i}",
                id_oficio=f"o{i}",
                numero_processo=f"p{i}",
                tipo_oficio="BLOQUEIO",
                status="success",
                duration_secs=5.0,
                decision_source="llm_fallback" if i < 4 else "deterministic",
            )
        )
    assert s.fallback_llm_count == 4
    assert s.fallback_rate == 0.4


# ---------------------------------------------------------------------------
# compute_slo_report
# ---------------------------------------------------------------------------


def test_slo_report_healthy_when_all_metrics_within_target():
    s = _make_summary([10] * 10)
    report = compute_slo_report(s)
    assert report.healthy
    assert report.violations == []


def test_slo_report_violates_p95():
    s = _make_summary([5, 5, 5, 5, 5, 5, 5, 5, 5, 90])
    report = compute_slo_report(s, SLOTargets(p95_seconds=60.0))
    assert not report.healthy
    names = [v.name for v in report.violations]
    assert "p95_seconds" in names


def test_slo_report_violates_error_rate():
    s = _make_summary(
        [10] * 10,
        statuses=["error"] * 5 + ["success"] * 5,
    )
    report = compute_slo_report(s, SLOTargets(error_rate=0.10))
    names = [v.name for v in report.violations]
    assert "error_rate" in names


def test_slo_report_violates_fallback_rate():
    s = RunSummary()
    for i in range(10):
        s.add(
            CaseRecord(
                lexia_id=f"LX-{i}",
                id_oficio=f"o{i}",
                numero_processo=f"p{i}",
                tipo_oficio="BLOQUEIO",
                status="success",
                duration_secs=5.0,
                decision_source="llm_fallback" if i < 5 else "deterministic",
            )
        )
    report = compute_slo_report(s, SLOTargets(fallback_rate=0.30))
    names = [v.name for v in report.violations]
    assert "fallback_rate" in names


def test_summary_to_dict_is_json_serializable():
    import json

    s = _make_summary([10, 20, 30])
    s.cases[0].error_category = ErrorCategory.NONE
    payload = s.to_dict()
    json.dumps(payload)
    assert payload["totals"]["total"] == 3
    assert "duration_secs" in payload
    assert "decision" in payload
