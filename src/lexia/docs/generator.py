"""Google Docs/Drive — call Apps Script to copy template and fill placeholders."""

from __future__ import annotations

import httpx
import structlog

from lexia.config import settings

log = structlog.get_logger(__name__)


async def generate_letter(
    doc_name: str,
    replacements: dict[str, str],
) -> dict:
    """Call the Apps Script Web App to generate a response letter.

    The Apps Script copies the Google Docs template (preserving 100% of layout)
    and replaces all placeholders inside the target Drive folder.

    Returns dict with doc_id and doc_url.
    """
    if not settings.apps_script_url:
        raise RuntimeError(
            "APPS_SCRIPT_URL not configured. Deploy the Apps Script and set the URL in .env"
        )

    payload = {
        "templateId": settings.google_template_doc_id,
        "folderId": settings.google_drive_folder_id,
        "docName": doc_name,
        "replacements": replacements,
    }

    log.info("apps_script_request", doc_name=doc_name, placeholders=len(replacements))

    async with httpx.AsyncClient(timeout=90, follow_redirects=True) as client:
        resp = await client.post(settings.apps_script_url, json=payload)
        resp.raise_for_status()
        result = resp.json()

    if "error" in result:
        raise RuntimeError(f"Apps Script error: {result['error']}")

    log.info("letter_generated", doc_url=result.get("docUrl"))
    return result
