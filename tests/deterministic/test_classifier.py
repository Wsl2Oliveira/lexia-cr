"""Tests for the deterministic classifier — covers all 13 macros + edge cases."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from lexia.deterministic.classifier import Confidence, classify  # noqa: E402


def _enrichment(
    *,
    nuconta_status: str | None = "open",
    nuconta_available: float | None = 0.0,
    rayquaza_total: float | None = None,
    rayquaza_caixinhas: float = 0.0,
    crebito_account_status: str | None = None,
    crebito_cards: list | None = None,
    crebito_balances: dict | None = None,
    blocks: list | None = None,
    customer_id: str = "abc-123",
    dados_bancarios: dict | None = None,
    assets: list | None = None,
) -> dict:
    """Build an enrichment dict matching what ``enrich_case`` returns."""
    return {
        "nuconta_status": json.dumps({"status": nuconta_status} if nuconta_status else {}),
        "nuconta_saldo": json.dumps({"available": nuconta_available} if nuconta_available is not None else {}),
        "rayquaza_saldo": json.dumps(
            {
                "caixinhas_total": rayquaza_caixinhas,
                "total_disponivel": rayquaza_total if rayquaza_total is not None else 0,
                "ativos": [],
            }
        ),
        "crebito_cartoes": json.dumps(
            {
                "account_status": crebito_account_status,
                "cards": crebito_cards or [],
                "balances": crebito_balances or {},
            }
        ),
        "blocks": blocks or [],
        "customers_customer_id": customer_id,
        "dados_bancarios": json.dumps(dados_bancarios or {}),
        "assets": assets or [],
        "waze_shard": "shard-1",
        "petrificus_bloqueios": json.dumps(blocks or []),
        "mario_box_caixinhas": json.dumps({"total": rayquaza_caixinhas}),
    }


def _case(tipo: str, *, info: list | None = None, cpf: str = "12345678901") -> dict:
    return {
        "tipo_oficio": f"official_letter_type__{tipo}",
        "info_solicitada": info or [],
        "cpf_cnpj": cpf,
        "nome_investigado": "Fulano Da Silva",
        "numero_processo": "0001234-56.2024.8.26.0100",
    }


# --------------------------------------------------------------------------- DESBLOQUEIO


def test_desbloqueio_sem_frozen_macro_1():
    case = _case("dismiss")
    enr = _enrichment()
    d = classify(case, enr)
    assert d.macro_id == "1"
    assert d.confidence is Confidence.HIGH


def test_desbloqueio_de_conta_com_frozen_macro_1b():
    case = _case("dismiss", info=["Desbloqueio De Conta"])
    enr = _enrichment(blocks=[{"status": "frozen", "frozen_amount": 500.0}])
    d = classify(case, enr)
    assert d.macro_id == "1B"
    assert d.frozen_amount == pytest.approx(500.0)


def test_desbloqueio_conta_sem_frozen_volta_macro_1():
    case = _case("dismiss", info=["Desbloqueio De Conta"])
    enr = _enrichment(blocks=[{"status": "dismissed", "frozen_amount": 0}])
    d = classify(case, enr)
    assert d.macro_id == "1"


def test_desbloqueio_com_restricao_comercial_marca_flag():
    case = _case("dismiss")
    enr = _enrichment(nuconta_status="internal_delinquent")
    d = classify(case, enr)
    assert d.macro_id == "1"
    assert d.requires_restricao_comercial is True


# --------------------------------------------------------------------------- BLOQUEIO


def test_bloqueio_sem_conta_sem_cartao_macro_2():
    case = _case("block")
    enr = _enrichment(nuconta_status="not_found", crebito_account_status="not_found", crebito_cards=[])
    d = classify(case, enr)
    assert d.macro_id == "2"


def test_bloqueio_saldo_infimo_macro_3():
    case = _case("block")
    enr = _enrichment(nuconta_available=2.50)
    d = classify(case, enr)
    assert d.macro_id == "3"


def test_bloqueio_padrao_macro_4():
    case = _case("block")
    enr = _enrichment(nuconta_available=1500.00)
    d = classify(case, enr)
    assert d.macro_id == "4"
    assert d.saldo_combinado == pytest.approx(1500.00)


def test_bloqueio_com_anterior_macro_5():
    case = _case("block")
    enr = _enrichment(
        nuconta_available=1500.00,
        blocks=[{"status": "frozen", "amount": 200.0}],
    )
    d = classify(case, enr)
    assert d.macro_id == "5"


def test_bloqueio_total_da_conta_macro_6():
    case = _case("block", info=["Bloqueio De Conta"])
    enr = _enrichment(nuconta_available=1500.00)
    d = classify(case, enr)
    assert d.macro_id == "6"


def test_bloqueio_so_cartao_sem_conta_macro_7():
    case = _case("block")
    enr = _enrichment(
        nuconta_status="not_found",
        crebito_account_status="not_found",
        crebito_cards=[{"id": "c1"}],
    )
    d = classify(case, enr)
    assert d.macro_id == "7"


def test_bloqueio_de_cartao_com_cards_macro_8():
    case = _case("block", info=["Bloqueio De Cartão"])
    enr = _enrichment(crebito_cards=[{"id": "c1"}])
    d = classify(case, enr)
    assert d.macro_id == "8"


def test_bloqueio_de_cartao_sem_cards_macro_9():
    case = _case("block", info=["Bloqueio De Cartão"])
    enr = _enrichment(crebito_cards=[])
    d = classify(case, enr)
    assert d.macro_id == "9"


# --------------------------------------------------------------------------- TRANSFERÊNCIA


def test_transferencia_nao_cliente_macro_t2():
    case = _case("transfer")
    enr = _enrichment(customer_id="NAO_CLIENTE")
    d = classify(case, enr)
    assert d.macro_id == "T2"
    assert d.is_nao_cliente is True


def test_transferencia_saldo_zero_macro_t1():
    case = _case("transfer")
    enr = _enrichment(nuconta_available=0.0)
    d = classify(case, enr)
    assert d.macro_id == "T1"


def test_transferencia_com_saldo_macro_t3():
    case = _case("transfer")
    enr = _enrichment(nuconta_available=2500.00)
    d = classify(case, enr)
    assert d.macro_id == "T3"


# --------------------------------------------------------------------------- Edge cases


def test_tipo_oficio_desconhecido_low_confidence():
    case = {"tipo_oficio": "official_letter_type__weird", "info_solicitada": [], "cpf_cnpj": "1"}
    enr = _enrichment()
    d = classify(case, enr)
    assert d.macro_id == ""
    assert d.confidence is Confidence.LOW


def test_dados_bancarios_request_marcado():
    case = _case("block", info=["Bloqueio De Valores", "Dados Bancarios"])
    enr = _enrichment(nuconta_available=1500.00, dados_bancarios={"agencia": "0001", "conta": "12345-6"})
    d = classify(case, enr)
    assert d.requires_dados_bancarios is True
    assert d.dados_bancarios == {"agencia": "0001", "conta": "12345-6"}


def test_doc_type_cnpj():
    case = _case("transfer", cpf="12345678000190")
    enr = _enrichment(customer_id="NAO_CLIENTE")
    d = classify(case, enr)
    assert d.doc_type == "CNPJ"
    assert d.macro_id == "T2"


def test_caixinhas_somam_no_saldo_combinado():
    case = _case("block")
    enr = _enrichment(nuconta_available=5.0, rayquaza_caixinhas=20.0)
    d = classify(case, enr)
    assert d.saldo_combinado == pytest.approx(25.0)
    assert d.macro_id == "4"


def test_info_solicitada_como_string_json():
    case = {
        "tipo_oficio": "official_letter_type__block",
        "info_solicitada": json.dumps(["Bloqueio De Cartão"]),
        "cpf_cnpj": "12345678901",
    }
    enr = _enrichment(crebito_cards=[{"id": "c1"}])
    d = classify(case, enr)
    assert d.macro_id == "8"


def test_enrichment_com_json_invalido_nao_quebra():
    case = _case("block")
    enr = {
        "nuconta_status": "{not json",
        "nuconta_saldo": "[]",
        "rayquaza_saldo": "",
        "crebito_cartoes": "null",
        "blocks": [],
        "customers_customer_id": "abc",
        "dados_bancarios": "",
    }
    d = classify(case, enr)
    assert d.macro_id != ""
