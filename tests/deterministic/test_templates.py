"""Snapshot-style tests for macro templates.

Asserts that each macro renders text starting with the canonical phrase
from the LLM prompt and that variable substitutions land in the right
spot. We deliberately don't compare full strings so wording tweaks are
local edits, not test fixture rewrites.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from lexia.deterministic.classifier import Confidence, DeterministicDecision  # noqa: E402
from lexia.deterministic.engine import decide  # noqa: E402
from lexia.deterministic.templates import render  # noqa: E402
from lexia.deterministic.value_words import format_brl, value_to_words_pt_br  # noqa: E402


def _d(macro_id: str, **overrides) -> DeterministicDecision:
    base = DeterministicDecision(
        macro_id=macro_id,
        macro_name="X",
        confidence=Confidence.HIGH,
        tipo_oficio="BLOQUEIO",
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_render_macro_1_basic():
    text = render(_d("1"))
    assert text.startswith("os valores anteriormente atingidos")
    assert "encontram-se ativos" in text


def test_render_macro_1_com_restricao_comercial_inclui_paragrafo_extra():
    text = render(_d("1", requires_restricao_comercial=True))
    assert "questões comerciais internas" in text


def test_render_macro_1b_substitui_valor_e_extenso():
    text = render(_d("1B", frozen_amount=1234.56))
    assert "R$ 1.234,56" in text
    assert "mil duzentos e trinta e quatro reais" in text
    assert "cinquenta e seis centavos" in text


def test_render_macro_2_fixo():
    assert render(_d("2")).startswith("inexiste conta ativa")


def test_render_macro_3_fixo():
    assert render(_d("3")).startswith("inexistem valores")


def test_render_macro_4_substitui_valor():
    text = render(_d("4", saldo_combinado=2_345.67))
    assert "R$ 2.345,67" in text
    assert "ativos de baixa liquidez" in text


def test_render_macro_5_substitui_valor_e_menciona_anteriores():
    text = render(_d("5", saldo_combinado=999.00))
    assert "R$ 999,00" in text
    assert "outros valores bloqueados" in text


def test_render_macro_6_substitui_saldo():
    text = render(_d("6", saldo_combinado=10.00))
    assert "R$ 10,00" in text
    assert "conta de pagamento foi bloqueada" in text


def test_render_macro_7_fixo():
    assert "inexiste conta ativa" in render(_d("7"))


def test_render_macro_8_fixo():
    assert "cartão de crédito foi bloqueado" in render(_d("8"))


def test_render_macro_9_fixo():
    text = render(_d("9"))
    assert text.startswith("inexiste cartão de crédito")


def test_render_macro_t1_fixo():
    text = render(_d("T1", tipo_oficio="TRANSFERÊNCIA"))
    assert "encontra-se zerada" in text


def test_render_macro_t2_substitui_doc():
    text = render(
        _d("T2", tipo_oficio="TRANSFERÊNCIA", doc_type="CNPJ", cpf_cnpj_raw="12345678000190")
    )
    assert "12345678000190" in text
    assert "CNPJ" in text


def test_render_macro_t3_lista_produtos():
    text = render(
        _d(
            "T3",
            tipo_oficio="TRANSFERÊNCIA",
            saldo_combinado=5_000.00,
            saldos_credito={"current_balance": 100.0, "overdue_balance": 50.0},
        )
    )
    assert "R$ 5.000,00" in text
    assert "Saldo em conta" in text
    assert "Fatura cartão de crédito" in text
    assert "R$ 150,00" in text
    assert "Não há empréstimos" in text


def test_render_macro_t3_sem_credito_diz_nao_ha_cartao():
    text = render(_d("T3", tipo_oficio="TRANSFERÊNCIA", saldo_combinado=100.0))
    assert "Não há cartão de crédito" in text


def test_render_macro_invalida_levanta_keyerror():
    with pytest.raises(KeyError):
        render(_d("INEXISTENTE"))


def test_dados_bancarios_paragraph_quando_disponivel():
    text = render(
        _d(
            "4",
            saldo_combinado=100.00,
            requires_dados_bancarios=True,
            dados_bancarios={"agencia": "0001", "conta": "12345-6"},
        )
    )
    assert "Banco Nu Pagamentos S.A. (260)" in text
    assert "Agência 0001" in text
    assert "Conta 12345-6" in text


def test_dados_bancarios_paragraph_omitido_quando_dados_vazios():
    text = render(
        _d(
            "4",
            saldo_combinado=100.00,
            requires_dados_bancarios=True,
            dados_bancarios={},
        )
    )
    assert "Banco Nu Pagamentos" not in text


# --------------------------------------------------------------------------- value_words


@pytest.mark.parametrize(
    ("v", "expected_substr"),
    [
        (1.00, "um real"),
        (2.00, "dois reais"),
        (0.50, "cinquenta centavos"),
        (100.00, "cem reais"),
        (1_000.00, "mil reais"),
        (1_234.56, "mil duzentos e trinta e quatro reais e cinquenta e seis centavos"),
    ],
)
def test_value_to_words_pt_br(v, expected_substr):
    assert value_to_words_pt_br(v) == expected_substr or expected_substr in value_to_words_pt_br(v)


@pytest.mark.parametrize(
    ("v", "expected"),
    [
        (1234.56, "1.234,56"),
        (0.5, "0,50"),
        (1_000_000.0, "1.000.000,00"),
    ],
)
def test_format_brl(v, expected):
    assert format_brl(v) == expected


# --------------------------------------------------------------------------- engine end-to-end


def test_engine_decide_drop_in_shape():
    case = {
        "tipo_oficio": "official_letter_type__dismiss",
        "info_solicitada": [],
        "cpf_cnpj": "12345678901",
    }
    import json as _json
    enr = {
        "nuconta_status": _json.dumps({"status": "open"}),
        "nuconta_saldo": _json.dumps({"available": 0}),
        "rayquaza_saldo": _json.dumps({"caixinhas_total": 0, "total_disponivel": 0, "ativos": []}),
        "crebito_cartoes": _json.dumps({"account_status": None, "cards": [], "balances": {}}),
        "blocks": [],
        "customers_customer_id": "abc",
        "dados_bancarios": _json.dumps({}),
        "assets": [],
        "waze_shard": "s",
        "petrificus_bloqueios": _json.dumps([]),
        "mario_box_caixinhas": _json.dumps({"total": 0}),
    }

    trace = decide(case, enr)

    assert set(["llm_macro_aplicada", "llm_id_macro", "llm_texto_resposta",
                "llm_observacoes", "llm_raw_response", "decision_source",
                "confidence", "decision_reason"]).issubset(trace.keys())
    assert trace["decision_source"] == "deterministic"
    assert trace["llm_id_macro"] == "1"
    assert trace["confidence"] == "HIGH"
    assert trace["llm_texto_resposta"].startswith("os valores anteriormente atingidos")
