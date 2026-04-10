"""Gemini integration — send case data with LexIA prompt, receive macro decision."""
from __future__ import annotations

import json
from dataclasses import dataclass

import google.generativeai as genai
import structlog

from lexia.config import settings

log = structlog.get_logger(__name__)


@dataclass
class LexiaDecision:
    """Structured output from the LexIA prompt."""

    macro_aplicada: str
    id_macro: str
    valor_bloqueio: str | None
    texto_resposta: str
    observacoes: str | None
    raw_response: str


LEXIA_SYSTEM_PROMPT = """\
Você é um analista regulatório especializado em ordens judiciais da Nubank.
Seu papel é analisar os dados do caso judicial e decidir qual macro de resposta aplicar.

Regras de decisão:
- Bloqueio com saldo <= R$10,00: Macro 3 (sem saldo)
- Bloqueio com valor solicitado e frozen existente: Macro com bloqueio parcial
- Desbloqueio: Macro de desbloqueio correspondente
- Transferência: Macro 9 (requer preenchimento manual de dados bancários)
- Reiteração: Avaliar se já foi respondido anteriormente
- Unificação de múltiplos investigados: id "UNIFICADO"

Formato de saída OBRIGATÓRIO (JSON):
{
    "macro_aplicada": "Nome da macro",
    "id_macro": "1-9 ou UNIFICADO",
    "valor_bloqueio": "valor em R$ ou null",
    "texto_resposta": "Texto completo da carta-resposta",
    "observacoes": "Observações adicionais ou null"
}

Responda APENAS com o JSON, sem texto adicional.
"""


def _build_case_context(
    case_data: dict,
    cards: list[dict],
    assets: list[dict],
    blocks: list[dict],
) -> str:
    """Build the context string that goes into the Gemini prompt."""
    return json.dumps(
        {
            "dados_caso": case_data,
            "cartoes_ativos": cards,
            "ativos_disponiveis": assets,
            "bloqueios_existentes": blocks,
        },
        ensure_ascii=False,
        indent=2,
    )


async def decide_macro(
    case_data: dict,
    cards: list[dict],
    assets: list[dict],
    blocks: list[dict],
    prompt_override: str | None = None,
) -> LexiaDecision:
    """Send case data to Gemini and get the macro decision.

    Args:
        case_data: Dict with judicial case fields.
        cards: Active cards from Crebito.
        assets: Available assets from Rayquaza.
        blocks: Existing freeze orders from Petrificus.
        prompt_override: Optional full prompt to use instead of the default.

    Returns:
        LexiaDecision with the macro and response text.
    """
    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel(
        model_name=settings.gemini_model,
        system_instruction=prompt_override or LEXIA_SYSTEM_PROMPT,
    )

    context = _build_case_context(case_data, cards, assets, blocks)
    user_message = f"Analise o caso abaixo e decida a macro:\n\n{context}"

    log.info("gemini_request", case_id=case_data.get("id", "?")[:8], model=settings.gemini_model)

    response = model.generate_content(user_message)
    raw = response.text.strip()

    try:
        cleaned = raw.removeprefix("```json").removesuffix("```").strip()
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        log.warning("gemini_parse_failed", raw=raw[:200])
        return LexiaDecision(
            macro_aplicada="ERRO_PARSE",
            id_macro="0",
            valor_bloqueio=None,
            texto_resposta=raw,
            observacoes="Falha ao parsear resposta do Gemini",
            raw_response=raw,
        )

    decision = LexiaDecision(
        macro_aplicada=parsed.get("macro_aplicada", "DESCONHECIDA"),
        id_macro=str(parsed.get("id_macro", "0")),
        valor_bloqueio=parsed.get("valor_bloqueio"),
        texto_resposta=parsed.get("texto_resposta", ""),
        observacoes=parsed.get("observacoes"),
        raw_response=raw,
    )

    log.info(
        "gemini_decision",
        case_id=case_data.get("id", "?")[:8],
        macro=decision.id_macro,
        macro_name=decision.macro_aplicada,
    )
    return decision
