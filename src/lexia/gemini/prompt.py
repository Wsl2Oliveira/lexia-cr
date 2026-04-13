"""LLM integration via LiteLLM — send case data with LexIA prompt, receive macro decision."""
from __future__ import annotations

import json
from dataclasses import dataclass

import structlog
from openai import OpenAI

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
    """Build the context string that goes into the prompt."""
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


def _get_client() -> OpenAI:
    return OpenAI(
        api_key=settings.litellm_api_key,
        base_url=settings.litellm_base_url,
    )


async def decide_macro(
    case_data: dict,
    cards: list[dict],
    assets: list[dict],
    blocks: list[dict],
    prompt_override: str | None = None,
) -> LexiaDecision:
    """Send case data to LLM and get the macro decision.

    Args:
        case_data: Dict with judicial case fields.
        cards: Active cards from Crebito.
        assets: Available assets from Rayquaza.
        blocks: Existing freeze orders from Petrificus.
        prompt_override: Optional full prompt to use instead of the default.

    Returns:
        LexiaDecision with the macro and response text.
    """
    client = _get_client()
    context = _build_case_context(case_data, cards, assets, blocks)
    user_message = f"Analise o caso abaixo e decida a macro:\n\n{context}"

    log.info("llm_request", case_id=case_data.get("id", "?")[:8], model=settings.litellm_model)

    response = client.chat.completions.create(
        model=settings.litellm_model,
        messages=[
            {"role": "system", "content": prompt_override or LEXIA_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.2,
        max_tokens=4096,
    )

    raw = response.choices[0].message.content.strip()

    try:
        cleaned = raw.removeprefix("```json").removesuffix("```").strip()
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        log.warning("llm_parse_failed", raw=raw[:200])
        return LexiaDecision(
            macro_aplicada="ERRO_PARSE",
            id_macro="0",
            valor_bloqueio=None,
            texto_resposta=raw,
            observacoes="Falha ao parsear resposta do LLM",
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
        "llm_decision",
        case_id=case_data.get("id", "?")[:8],
        macro=decision.id_macro,
        macro_name=decision.macro_aplicada,
    )
    return decision
