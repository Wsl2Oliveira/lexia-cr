"""Convert numeric values to PT-BR currency text (used by Macro 1B).

Implementation is dependency-free: a small recursive expansion adequate
for amounts up to R$ 999.999.999,99 (well above any judicial block we've
seen). If we ever need higher precision/coverage, swap for ``num2words``.
"""

from __future__ import annotations

_UNITS = [
    "zero", "um", "dois", "três", "quatro", "cinco", "seis", "sete", "oito", "nove",
    "dez", "onze", "doze", "treze", "quatorze", "quinze", "dezesseis",
    "dezessete", "dezoito", "dezenove",
]
_TENS = [
    "", "", "vinte", "trinta", "quarenta", "cinquenta",
    "sessenta", "setenta", "oitenta", "noventa",
]
_HUNDREDS = [
    "", "cento", "duzentos", "trezentos", "quatrocentos", "quinhentos",
    "seiscentos", "setecentos", "oitocentos", "novecentos",
]


def _three_digits_to_words(n: int) -> str:
    """Convert an integer in [0, 999] to PT-BR words (no leading 'e')."""
    if n == 0:
        return ""
    if n == 100:
        return "cem"

    parts: list[str] = []
    h = n // 100
    rest = n % 100

    if h > 0:
        parts.append(_HUNDREDS[h])

    if rest < 20:
        if rest > 0:
            parts.append(_UNITS[rest])
    else:
        t = rest // 10
        u = rest % 10
        ten_word = _TENS[t]
        if u == 0:
            parts.append(ten_word)
        else:
            parts.append(f"{ten_word} e {_UNITS[u]}")

    return " e ".join(parts)


def _integer_to_words(n: int) -> str:
    if n == 0:
        return "zero"
    if n < 0:
        return f"menos {_integer_to_words(-n)}"

    millions = n // 1_000_000
    thousands = (n % 1_000_000) // 1_000
    units = n % 1_000

    chunks: list[str] = []

    if millions > 0:
        if millions == 1:
            chunks.append("um milhão")
        else:
            chunks.append(f"{_three_digits_to_words(millions)} milhões")

    if thousands > 0:
        if thousands == 1:
            chunks.append("mil")
        else:
            chunks.append(f"{_three_digits_to_words(thousands)} mil")

    if units > 0:
        chunks.append(_three_digits_to_words(units))

    if not chunks:
        return "zero"

    if len(chunks) == 1:
        return chunks[0]

    last = chunks[-1]
    needs_e = (units > 0 and units < 100) or (units > 0 and units % 100 == 0 and units < 1000)
    if needs_e:
        return " ".join(chunks[:-1]) + " e " + last
    return " ".join(chunks)


def value_to_words_pt_br(value: float) -> str:
    """Convert a monetary amount to PT-BR words.

    Examples:
        >>> value_to_words_pt_br(1234.56)
        'mil duzentos e trinta e quatro reais e cinquenta e seis centavos'
        >>> value_to_words_pt_br(1.0)
        'um real'
        >>> value_to_words_pt_br(0.50)
        'cinquenta centavos'
    """
    cents_total = round(value * 100)
    reais = cents_total // 100
    cents = cents_total % 100

    parts: list[str] = []

    if reais > 0:
        reais_word = _integer_to_words(reais)
        parts.append(f"{reais_word} {'real' if reais == 1 else 'reais'}")

    if cents > 0:
        cents_word = _integer_to_words(cents)
        parts.append(f"{cents_word} {'centavo' if cents == 1 else 'centavos'}")

    if not parts:
        return "zero real"

    return " e ".join(parts)


def format_brl(value: float) -> str:
    """Format a number as BRL currency string (e.g., ``1234,56``)."""
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
