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


@pytest.mark.parametrize(
    ("vara", "orgao", "expected_vara", "expected_orgao"),
    [
        (
            "Poder Judiciário TJ-RS 2ª Vara Cível de Bento Gonçalves",
            "Poder Judiciário TJ-RS 2ª Vara Cível de Bento Gonçalves",
            "Poder Judiciário TJ-RS 2ª Vara Cível de Bento Gonçalves",
            "",
        ),
        (
            "  Poder Judiciário TJ-RS  ",
            "poder judiciário tj-rs",
            "  Poder Judiciário TJ-RS  ",
            "",
        ),
        (
            "TJ-RS — 2ª Vara Cível de Bento Gonçalves",
            "TJ-RS",
            "TJ-RS — 2ª Vara Cível de Bento Gonçalves",
            "",
        ),
        (
            "TJ-RS",
            "TJ-RS — 2ª Vara Cível de Bento Gonçalves",
            "TJ-RS — 2ª Vara Cível de Bento Gonçalves",
            "",
        ),
        (
            "1ª Vara Cível de São Paulo",
            "Tribunal de Justiça do Estado de São Paulo",
            "1ª Vara Cível de São Paulo",
            "Tribunal de Justiça do Estado de São Paulo",
        ),
        ("Vara X", "", "Vara X", ""),
        ("", "Órgão Y", "", "Órgão Y"),
    ],
)
def test_dedupe_orgao(vara, orgao, expected_vara, expected_orgao):
    out_vara, out_orgao = rtp._dedupe_orgao(vara, orgao)
    assert out_vara == expected_vara
    assert out_orgao == expected_orgao


def test_build_replacements_dedups_duplicated_header():
    ref = {
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "Poder Judiciário TJ-RS 2ª Vara Cível de Bento Gonçalves",
        "orgao_nome": "Poder Judiciário TJ-RS 2ª Vara Cível de Bento Gonçalves",
    }
    invs = [
        {
            "case": {"nome_investigado": "Fulano", "cpf_cnpj": "12345678901"},
            "enrichment": {},
            "llm_trace": {"id_macro": "1", "macro_aplicada": "Macro X", "texto_resposta": "ok."},
        },
    ]
    replacements, _, _ = rtp.build_generate_doc_replacements(ref, invs)
    assert replacements["{{Vara/Seccional}}"].endswith("Bento Gonçalves")
    assert replacements["{{Órgão (delegacia/tribunal)}}"] == ""


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
    """Mesma macro: cada investigado adicional precisa receber seu próprio
    bloco "Em relação a ..." no campo da macro, e o campo "documento" deve
    conter apenas o CPF do primeiro (os demais entram no bloco da macro)."""
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
    doc_field = replacements["{{documento do cliente atingido}}"]
    assert doc_field == "123.456.789-01"
    assert "Bia" not in doc_field
    macro_block = replacements["{{macro da operação realizada}}"]
    assert macro_block.lower().startswith("há conta ativa em seu nome.")
    assert "Em relação a Bia - CPF n.º 987.654.321-00" in macro_block
    assert macro_block.lower().count("há conta ativa em seu nome.") == 2
    assert len(bold_texts) == 2


def test_generate_doc_replacements_same_macro_id_distinct_texts():
    """Bug histórico (linha 52 da prod 23/04/2026): macro_id igual mas textos
    diferentes (saldos distintos por investigado) faziam o texto do segundo
    sumir. Agora cada investigado precisa aparecer com seu próprio texto."""
    ref = {
        "tipo_oficio": "official_letter_type__block",
        "numero_processo": "0001",
        "numero_oficio": "OF-1",
        "vara_tribunal": "S/N",
        "orgao_nome": "S/N",
    }
    invs = [
        _inv(
            "EZEQUIAS LTDA",
            "09651926000111",
            macro_text="foi bloqueado o importe disponível de R$ 38,27.",
            macro_id="4",
        ),
        _inv(
            "Ezequias",
            "30310955882",
            macro_text="foi bloqueado o importe disponível de R$ 422,26.",
            macro_id="4",
        ),
    ]
    replacements, _bold, _ = rtp.build_generate_doc_replacements(ref, invs)
    macro_block = replacements["{{macro da operação realizada}}"]
    assert "R$ 38,27" in macro_block
    assert "R$ 422,26" in macro_block, (
        "Texto diferente do segundo investigado precisa aparecer no doc"
    )
    assert "Em relação a Ezequias - CPF n.º 303.109.558-82" in macro_block


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
