"""Generate example letters by calling the Apps Script Web App.

Usage:
    python scripts/generate_examples.py

Requires APPS_SCRIPT_URL in .env (deploy the Apps Script first).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lexia.config import settings

TEMPLATE_UNICO = os.environ.get("LEXIA_TEMPLATE_UNICO", settings.google_template_doc_id)
TEMPLATE_MULTIPLOS = os.environ.get("LEXIA_TEMPLATE_MULTIPLOS", "")
TARGET_FOLDER = settings.google_drive_folder_id

EXAMPLES: list[dict] = [
    {
        "doc_name": "CR-EXEMPLO-BLOQUEIO-0000001-00.2026.0.00.0001",
        "template_id": TEMPLATE_UNICO,
        "tipo": "bloqueio",
        "subtipo": "bloqueio_conta_bloqueada",
        "replacements": {
            "{{data da elaboração deste documento}}": "13 de abril de 2026",
            "{{número do ofício}}": "0001/2026",
            "{{número do processo}}": "0000001-00.2026.0.00.0001",
            "{{Vara/Seccional}}": "1ª Vara Cível de São Paulo",
            "{{Órgão (delegacia/tribunal)}}": "Tribunal de Justiça do Estado de São Paulo",
            "{{NOME DO CLIENTE ATINGIDO}}": "FULANO DE TAL",
            "CPF (CNPJ)": "CPF",
            "{{documento do cliente atingido}}": "000.000.000-00",
            "{{macro da operação realizada}}": (
                "em cumprimento ao ofício judicial, realizamos o bloqueio judicial "
                "no valor de R$ 1.000,00 na conta do(a) cliente."
            ),
        },
    },
    {
        "doc_name": "CR-EXEMPLO-BLOQUEIO-0000002-00.2026.0.00.0002",
        "template_id": TEMPLATE_UNICO,
        "tipo": "bloqueio",
        "subtipo": "saldo_irrisorio_bacenjud",
        "replacements": {
            "{{data da elaboração deste documento}}": "13 de abril de 2026",
            "{{número do ofício}}": "S/N",
            "{{número do processo}}": "0000002-00.2026.0.00.0002",
            "{{Vara/Seccional}}": "2ª Vara do Trabalho de São Paulo",
            "{{Órgão (delegacia/tribunal)}}": "Tribunal Regional do Trabalho da 2ª Região",
            "{{NOME DO CLIENTE ATINGIDO}}": "CICLANA DE EXEMPLO",
            "CPF (CNPJ)": "CPF",
            "{{documento do cliente atingido}}": "111.111.111-11",
            "{{macro da operação realizada}}": (
                "o saldo disponível na conta do(a) cliente na data desta resposta "
                "é de R$ 0,01, desta forma e, observando o que consta no art. 13 "
                "§ 10 do Regulamento Bacenjud 2.0: As instituições participantes "
                "ficam dispensadas de efetivar o bloqueio e transferência quando o "
                "saldo consolidado atingido for igual ou inferior a R$ 10,00 (dez "
                "reais). Desta forma, o bloqueio e transferência de valores restam "
                "prejudicados."
            ),
        },
    },
    {
        "doc_name": "CR-EXEMPLO-DESBLOQUEIO-0000003-00.2026.0.00.0003",
        "template_id": TEMPLATE_UNICO,
        "tipo": "desbloqueio",
        "subtipo": "desbloqueio_produtos_livres",
        "replacements": {
            "{{data da elaboração deste documento}}": "13 de abril de 2026",
            "{{número do ofício}}": "0003/2026",
            "{{número do processo}}": "0000003-00.2026.0.00.0003",
            "{{Vara/Seccional}}": "Juizado Especial Cível e Criminal",
            "{{Órgão (delegacia/tribunal)}}": "Tribunal de Justiça do Estado de São Paulo",
            "{{NOME DO CLIENTE ATINGIDO}}": "BELTRANO DA SILVA",
            "CPF (CNPJ)": "CPF",
            "{{documento do cliente atingido}}": "222.222.222-22",
            "{{macro da operação realizada}}": (
                "os produtos deste cliente, atingidos pela determinação encaminhada "
                "por esse eg. Tribunal, encontram-se ativos e livres de qualquer "
                "bloqueio judicial vinculado aos presentes autos, na data desta resposta."
            ),
        },
    },
]


async def call_apps_script(payload: dict) -> dict:
    """POST to the Apps Script Web App and return the JSON response."""
    if not settings.apps_script_url:
        raise RuntimeError(
            "APPS_SCRIPT_URL não configurada. "
            "Faça o deploy do Apps Script e configure a URL no .env"
        )

    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.post(settings.apps_script_url, json=payload)
        resp.raise_for_status()
        result = resp.json()

    if "error" in result:
        raise RuntimeError(f"Apps Script error: {result['error']}")

    return result


async def generate_example(example: dict) -> dict:
    """Generate a single example letter via Apps Script."""
    payload = {
        "templateId": example["template_id"],
        "folderId": TARGET_FOLDER,
        "docName": example["doc_name"],
        "replacements": example["replacements"],
    }

    print(f"\n{'='*60}")
    print(f"Gerando: {example['doc_name']}")
    print(f"  Tipo: {example['tipo']} / {example['subtipo']}")
    print(f"  Template: {'único atingido' if example['template_id'] == TEMPLATE_UNICO else 'múltiplos atingidos'}")

    result = await call_apps_script(payload)

    print(f"  ✓ Doc: {result.get('docUrl')}")

    return {**example, **result}


async def main():
    print("=" * 60)
    print("LexIA CR — Gerador de Cartas-Exemplo via Apps Script")
    print(f"Apps Script URL: {settings.apps_script_url[:50]}..." if settings.apps_script_url else "Apps Script URL: NÃO CONFIGURADA")
    print(f"Pasta destino: {TARGET_FOLDER}")
    print(f"Total de exemplos: {len(EXAMPLES)}")
    print("=" * 60)

    if not settings.apps_script_url:
        print("\n❌ APPS_SCRIPT_URL não está configurada no .env")
        print("   Siga as instruções de deploy do Apps Script primeiro.")
        sys.exit(1)

    results = []
    for example in EXAMPLES:
        result = await generate_example(example)
        results.append(result)

    output_path = Path(__file__).parent.parent / "data" / "generated_examples.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    print(f"\n{'='*60}")
    print(f"✓ {len(results)} cartas geradas com sucesso!")
    print(f"  Resultados salvos em: {output_path}")
    print(f"  Pasta Drive: https://drive.google.com/drive/folders/{TARGET_FOLDER}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
