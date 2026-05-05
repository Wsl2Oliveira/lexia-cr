"""Macro text templates for LexIA deterministic engine.

Each macro has a render function ``render_<id>(decision) -> str`` returning
the body of ``texto_resposta``. Texts are copied verbatim from the LLM
system prompt in ``scripts/run_traced_pipeline.py`` (the canonical source
of macro wording).

The dispatcher :func:`render` applies orthogonal additives:
    - "+ Restrição comercial" (Macro 1 with commercial-blocked account)
    - "+ Dados bancários" (info_solicitada contains "Dados Bancários")
"""

from __future__ import annotations

from lexia.deterministic.classifier import DeterministicDecision
from lexia.deterministic.value_words import format_brl, value_to_words_pt_br


# ---------------------------------------------------------------------------
# Block / unblock macros (1, 1B, 2..9)
# ---------------------------------------------------------------------------


def render_macro_1(d: DeterministicDecision) -> str:
    """DESBLOQUEIO REALIZADO."""
    return (
        "os valores anteriormente atingidos por determinação judicial, "
        "encontram-se ativos e livres de qualquer bloqueio vinculado aos "
        "presentes autos."
    )


def render_macro_1b(d: DeterministicDecision) -> str:
    """DESBLOQUEIO DE CONTA COM VALORES CONSTRITOS."""
    valor_str = format_brl(d.frozen_amount)
    extenso = value_to_words_pt_br(d.frozen_amount)
    return (
        "a conta de pagamento encontra-se ativa e livre para movimentação, "
        "conforme determinado.\n\n"
        f"Cumpre esclarecer que permanecem constritos os valores de R$ {valor_str} "
        f"({extenso}), bloqueados por força de determinação judicial prolatada "
        "anteriormente nos presentes autos, os quais não foram objeto da ordem "
        "de desbloqueio."
    )


def render_macro_2(d: DeterministicDecision) -> str:
    """NÃO POSSUI CONTA ATIVA."""
    return (
        "inexiste conta ativa em seu nome, pelo que resta inviabilizado o "
        "cumprimento da ordem."
    )


def render_macro_3(d: DeterministicDecision) -> str:
    """SALDO ZERADO OU VALOR ÍNFIMO."""
    return (
        "inexistem valores passíveis de bloqueio, pelo que resta inviabilizado "
        "o cumprimento da ordem."
    )


def render_macro_4(d: DeterministicDecision) -> str:
    """BLOQUEIO DE VALOR IGUAL AO DETERMINADO."""
    valor_str = format_brl(d.saldo_combinado)
    return (
        f"foi bloqueado o importe disponível de R$ {valor_str}.\n\n"
        "Cumpre esclarecer que o valor bloqueado pode incluir ativos de baixa "
        "liquidez e/ou sujeitos a variações de mercado em razão da própria "
        "natureza do investimento no qual o importe está alocado. Por isso, "
        "caso a transferência desses valores seja determinada futuramente, o "
        "montante final poderá ser alterado, momento em que serão informadas "
        "as especificidades, caso existam."
    )


def render_macro_5(d: DeterministicDecision) -> str:
    """EXISTÊNCIA DE BLOQUEIO ANTERIOR + BLOQUEIO DE VALOR IGUAL AO DETERMINADO."""
    valor_str = format_brl(d.saldo_combinado)
    return (
        f"foi bloqueado o importe disponível de R$ {valor_str} em benefício "
        "deste processo, existindo, ainda, outros valores bloqueados em razão "
        "de determinações judiciais prolatadas anteriormente.\n\n"
        "Cumpre esclarecer que o valor bloqueado pode incluir ativos de baixa "
        "liquidez e/ou sujeitos a variações de mercado em razão da própria "
        "natureza do investimento no qual o importe está alocado. Por isso, "
        "caso a transferência desses valores seja determinada futuramente, o "
        "montante final poderá ser alterado, momento em que serão informadas "
        "as especificidades, caso existam."
    )


def render_macro_6(d: DeterministicDecision) -> str:
    """BLOQUEIO TOTAL DA CONTA DE PAGAMENTO."""
    valor_str = format_brl(d.saldo_combinado)
    return (
        "a conta de pagamento foi bloqueada, nesta data com saldo de "
        f"R$ {valor_str}."
    )


def render_macro_7(d: DeterministicDecision) -> str:
    """CLIENTE NÃO TEM CONTA DO NUBANK, SÓ TEM CARTÃO DE CRÉDITO."""
    return (
        "inexiste conta ativa em seu nome, pelo que resta inviabilizado o "
        "cumprimento da ordem."
    )


def render_macro_8(d: DeterministicDecision) -> str:
    """BLOQUEIO DE CARTÃO DE CRÉDITO."""
    return (
        "o cartão de crédito foi bloqueado, bem como nosso sistema foi "
        "parametrizado para a não liberação de novo cartão com a função "
        "crédito."
    )


def render_macro_9(d: DeterministicDecision) -> str:
    """NEGATIVA BLOQUEIO DE CARTÃO DE CRÉDITO."""
    return (
        "inexiste cartão de crédito nesta instituição na presente data. "
        "Informamos também que o nosso sistema está parametrizado para a não "
        "liberação de cartão com a função crédito."
    )


# ---------------------------------------------------------------------------
# Transfer macros (T1, T2, T3)
# ---------------------------------------------------------------------------


def render_macro_t1(d: DeterministicDecision) -> str:
    """CONTA ZERADA (TRANSFERÊNCIA INVIÁVEL)."""
    return (
        "a conta do(a) cliente supra encontra-se zerada na data desta "
        "resposta. Em função do exposto, o atendimento à ordem judicial de "
        "transferência e depósito em juízo resta prejudicado por inexistência "
        "de saldo."
    )


def render_macro_t2(d: DeterministicDecision) -> str:
    """NÃO É CLIENTE (TRANSFERÊNCIA)."""
    return (
        f"o {d.doc_type} referido no ofício é o de número {d.cpf_cnpj_raw}, "
        f"vimos informar por meio desta que esse {d.doc_type} não consta em "
        "nossos cadastros de clientes."
    )


def _t3_fatura_value(d: DeterministicDecision) -> str:
    """Compute fatura total = a vencer + vencido (best-effort from saldos_credito)."""
    saldos = d.saldos_credito or {}

    def _f(key_options: list[str]) -> float:
        for k in key_options:
            v = saldos.get(k)
            if v is not None and v != "":
                try:
                    return float(v)
                except (ValueError, TypeError):
                    continue
        return 0.0

    a_vencer = _f(["current_balance", "open_balance", "to_due", "a_vencer"])
    vencido = _f(["overdue_balance", "past_due", "vencido"])
    total = a_vencer + vencido
    return format_brl(total)


def render_macro_t3(d: DeterministicDecision) -> str:
    """TRANSFERÊNCIA REALIZADA — detalhamento de produtos."""
    saldo_str = format_brl(d.saldo_combinado)
    fatura_str = _t3_fatura_value(d)

    has_assets = bool(d.assets)
    has_credit = bool(d.saldos_credito)

    lines = [
        "em cumprimento à referida ordem judicial, comunicamos que o(a) "
        "cliente supra possui as seguintes posições na data desta resposta:",
        "",
        f"- Saldo em conta: R$ {saldo_str}",
    ]

    if has_credit:
        lines.append(
            f"- Fatura cartão de crédito (Valor a vencer + Valor vencido) R$ {fatura_str}"
        )
    else:
        lines.append("- Não há cartão de crédito")

    lines.append("- Não há empréstimos")

    if has_assets:
        lines.append("- Investimentos: vide extrato anexo")
    else:
        lines.append("- Não há investimentos")

    lines.append("- Não há seguro de vida")
    lines.append("- Não há criptoativos")

    lines.extend(
        [
            "",
            "Ademais, oportuno esclarecer que as informações supra, referem-se "
            "à totalidade de produtos contratados pelo(a) cliente na data "
            "desta resposta.",
        ]
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Dispatcher + additives
# ---------------------------------------------------------------------------


MACRO_TEMPLATES = {
    "1": render_macro_1,
    "1B": render_macro_1b,
    "2": render_macro_2,
    "3": render_macro_3,
    "4": render_macro_4,
    "5": render_macro_5,
    "6": render_macro_6,
    "7": render_macro_7,
    "8": render_macro_8,
    "9": render_macro_9,
    "T1": render_macro_t1,
    "T2": render_macro_t2,
    "T3": render_macro_t3,
}


_RESTRICAO_COMERCIAL_PARAGRAPH = (
    "Informamos que eventuais restrições nos produtos do cliente decorrem de "
    "questões comerciais internas, não relacionadas à ordem judicial."
)


def _dados_bancarios_paragraph(d: DeterministicDecision) -> str | None:
    db = d.dados_bancarios or {}
    agencia = db.get("agencia") or db.get("branch")
    conta = db.get("conta") or db.get("account") or db.get("number")
    if not agencia or not conta:
        return None
    return (
        f"Informamos os dados da conta de pagamento: Banco Nu Pagamentos S.A. "
        f"(260), Agência {agencia}, Conta {conta}."
    )


def render(decision: DeterministicDecision) -> str:
    """Render the full ``texto_resposta`` for a deterministic decision.

    Includes orthogonal additives:
        - Restrição comercial paragraph (Macro 1 only).
        - Dados bancários paragraph (whenever requested and available).

    Raises:
        KeyError: If the macro_id is not recognized.
    """
    if decision.macro_id not in MACRO_TEMPLATES:
        raise KeyError(
            f"Macro '{decision.macro_id}' não possui template registrado. "
            "Verifique o classifier."
        )

    body = MACRO_TEMPLATES[decision.macro_id](decision)

    extras: list[str] = []

    if decision.macro_id == "1" and decision.requires_restricao_comercial:
        extras.append(_RESTRICAO_COMERCIAL_PARAGRAPH)

    if decision.requires_dados_bancarios:
        db_paragraph = _dados_bancarios_paragraph(decision)
        if db_paragraph:
            extras.append(db_paragraph)

    if not extras:
        return body

    return body + "\n\n" + "\n\n".join(extras)
