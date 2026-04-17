"""Tests for LexIA pipeline core functions."""

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import run_traced_pipeline as rtp  # noqa: E402, I001


LEXIA_ID_PATTERN = re.compile(r"^LX-[A-Z0-9]{4}-[A-Z0-9]{4}$")


def test_generate_lexia_id_format():
    lexia_id = rtp.generate_lexia_id()
    assert LEXIA_ID_PATTERN.match(lexia_id), lexia_id


def test_generate_lexia_id_uniqueness():
    ids = {rtp.generate_lexia_id() for _ in range(100)}
    assert len(ids) == 100


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12345678901", "123.456.789-01"),
        ("12345678000190", "12.345.678/0001-90"),
        ("123.456.789-01", "123.456.789-01"),
        ("12.345.678/0001-90", "12.345.678/0001-90"),
    ],
)
def test_format_cpf(raw, expected):
    assert rtp._format_cpf(raw) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "S/N"),
        ("", "S/N"),
        ("None", "S/N"),
        ("hello", "hello"),
    ],
)
def test_sanitize(value, expected):
    assert rtp._sanitize(value) == expected


def test_clean_macro_text_strips_prefix_and_lowercases():
    raw = "Em atenção ao ofício judicial, informamos que Inexistem valores."
    out = rtp._clean_macro_text(raw)
    assert not out.lower().startswith("em atenção ao ofício judicial")
    assert out.startswith("inexistem")


def test_clean_macro_text_pass_through():
    assert rtp._clean_macro_text("já começa em minúsculas.") == "já começa em minúsculas."


def test_group_by_id_oficio_two_rows_same_oficio_two_investigados():
    cases = [
        {"id_oficio": "o1", "nome_investigado": "A", "cpf_cnpj": "1"},
        {"id_oficio": "o1", "nome_investigado": "B", "cpf_cnpj": "2"},
    ]
    grouped = rtp.group_cases_by_oficio(cases)
    assert len(grouped) == 1
    assert grouped[0]["id_oficio"] == "o1"
    assert len(grouped[0]["investigados"]) == 2


def test_group_by_id_oficio_dedup_same_nome_cpf():
    cases = [
        {"id_oficio": "o1", "nome_investigado": "A", "cpf_cnpj": "1"},
        {"id_oficio": "o1", "nome_investigado": "A", "cpf_cnpj": "1"},
    ]
    grouped = rtp.group_cases_by_oficio(cases)
    assert len(grouped) == 1
    assert len(grouped[0]["investigados"]) == 1


def test_group_by_id_oficio_two_distinct_oficios():
    cases = [
        {"id_oficio": "o1", "nome_investigado": "A", "cpf_cnpj": "1"},
        {"id_oficio": "o1", "nome_investigado": "B", "cpf_cnpj": "2"},
        {"id_oficio": "o2", "nome_investigado": "C", "cpf_cnpj": "3"},
    ]
    grouped = rtp.group_cases_by_oficio(cases)
    assert len(grouped) == 2
    assert {g["id_oficio"] for g in grouped} == {"o1", "o2"}


def test_group_by_id_oficio_single_row():
    cases = [{"id_oficio": "o1", "nome_investigado": "A", "cpf_cnpj": "1"}]
    grouped = rtp.group_cases_by_oficio(cases)
    assert len(grouped) == 1
    assert len(grouped[0]["investigados"]) == 1
    assert grouped[0]["ref_case"]["nome_investigado"] == "A"


def _inv(
    nome: str,
    cpf: str,
    *,
    macro_text: str,
    macro_id: str,
    customer_id: str = "cust-1",
    macro_aplicada: str = "M1",
):
    return {
        "case": {"nome_investigado": nome, "cpf_cnpj": cpf},
        "enrichment": {"customers_customer_id": customer_id},
        "llm_trace": {
            "llm_texto_resposta": macro_text,
            "llm_macro_aplicada": macro_aplicada,
            "llm_id_macro": macro_id,
        },
    }


def test_generate_doc_replacements_single_investigado():
    ref = {
        "tipo_oficio": "official_letter_type__block",
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "Vara X",
        "orgao_nome": "Órgão Y",
    }
    replacements, bold_texts, doc_name = rtp.build_generate_doc_replacements(
        ref,
        [_inv("João Silva", "12345678901", macro_text="texto macro.", macro_id="M1")],
    )
    assert doc_name.startswith("CR-0001-")
    assert replacements["{{NOME DO CLIENTE ATINGIDO}}"] == "João Silva"
    assert replacements["{{documento do cliente atingido}}"] == "123.456.789-01"
    assert replacements["{{macro da operação realizada}}"] == "texto macro."
    assert bold_texts == ["João Silva - CPF n.º 123.456.789-01"]


def test_generate_doc_replacements_same_macro_multiple_investigados():
    ref = {
        "tipo_oficio": "official_letter_type__block",
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "S/N",
        "orgao_nome": "S/N",
    }
    macro = "Há conta ativa em seu nome."
    invs = [
        _inv("Ana", "12345678901", macro_text=macro, macro_id="M1"),
        _inv("Bia", "98765432100", macro_text=macro, macro_id="M1"),
    ]
    replacements, bold_texts, _doc_name = rtp.build_generate_doc_replacements(ref, invs)
    assert "nome dos investigados" in replacements["{{macro da operação realizada}}"].lower()
    doc_field = replacements["{{documento do cliente atingido}}"]
    assert "123.456.789-01" in doc_field
    assert "Bia" in doc_field
    assert len(bold_texts) == 2


def test_generate_doc_replacements_different_macros():
    ref = {
        "tipo_oficio": "official_letter_type__block",
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "S/N",
        "orgao_nome": "S/N",
    }
    invs = [
        _inv("Ana", "12345678901", macro_text="primeira resposta.", macro_id="M1"),
        _inv("Bia", "98765432100", macro_text="segunda resposta.", macro_id="M2"),
    ]
    replacements, _bold, _ = rtp.build_generate_doc_replacements(ref, invs)
    macro_block = replacements["{{macro da operação realizada}}"]
    assert "primeira resposta." in macro_block
    assert "Em relação a Bia" in macro_block
    assert "segunda resposta." in macro_block


def test_generate_doc_replacements_mixed_client_and_non_client_collective():
    ref = {
        "tipo_oficio": "official_letter_type__block",
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "S/N",
        "orgao_nome": "S/N",
    }
    invs = [
        _inv("Cliente Nu", "12345678901", macro_text="macro do cliente.", macro_id="M1"),
        _inv(
            "Outro",
            "98765432100",
            macro_text="macro ignorada para não cliente.",
            macro_id="M2",
            customer_id="NAO_CLIENTE",
        ),
    ]
    replacements, _bold, _ = rtp.build_generate_doc_replacements(ref, invs)
    macro_block = replacements["{{macro da operação realizada}}"]
    assert "macro do cliente." in macro_block
    assert "não identificamos em nossa base de clientes" in macro_block
    assert "Outro" in macro_block
    assert "macro ignorada" not in macro_block
