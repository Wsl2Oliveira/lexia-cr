"""Google Docs/Drive — copy template, fill placeholders, export PDF, save."""
from __future__ import annotations

from pathlib import Path

import structlog
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from lexia.config import settings

log = structlog.get_logger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

_docs_service = None
_drive_service = None


def _get_creds() -> Credentials:
    creds_path = Path(settings.google_credentials_path).expanduser()
    if not creds_path.exists():
        raise FileNotFoundError(f"Google credentials not found at {creds_path}")
    return Credentials.from_service_account_file(str(creds_path), scopes=_SCOPES)


def _get_docs():
    global _docs_service
    if _docs_service is None:
        _docs_service = build("docs", "v1", credentials=_get_creds())
    return _docs_service


def _get_drive():
    global _drive_service
    if _drive_service is None:
        _drive_service = build("drive", "v3", credentials=_get_creds())
    return _drive_service


def copy_template(doc_name: str) -> str:
    """Copy the Google Docs template and return the new document ID."""
    drive = _get_drive()
    body = {
        "name": doc_name,
        "parents": [settings.google_drive_folder_id],
    }
    copy = drive.files().copy(fileId=settings.google_template_doc_id, body=body).execute()
    doc_id = copy["id"]
    log.info("template_copied", doc_id=doc_id, name=doc_name)
    return doc_id


_PLAIN_TEXT_KEYS = {"data da elaboração deste documento", "CPF (CNPJ)"}


def fill_placeholders(doc_id: str, replacements: dict[str, str]) -> None:
    """Replace placeholders in the document with actual values.

    Handles two formats:
      - {{key}} — standard bracketed placeholders
      - plain text — literal text in the doc (e.g. "CPF (CNPJ)")
    """
    docs = _get_docs()
    requests = []

    for key, value in replacements.items():
        if key in _PLAIN_TEXT_KEYS:
            search_text = key
        else:
            search_text = f"{{{{{key}}}}}"

        requests.append({
            "replaceAllText": {
                "containsText": {"text": search_text, "matchCase": False},
                "replaceText": value or "",
            }
        })

    if not requests:
        return

    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests},
    ).execute()

    log.info("placeholders_filled", doc_id=doc_id, count=len(requests))


def export_pdf(doc_id: str, output_path: Path | None = None) -> bytes:
    """Export the Google Doc as PDF. Optionally save to local path.

    Returns the PDF bytes.
    """
    import io

    drive = _get_drive()
    request = drive.files().export_media(fileId=doc_id, mimeType="application/pdf")

    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    pdf_bytes = buffer.getvalue()

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(pdf_bytes)
        log.info("pdf_saved", path=str(output_path), size_kb=len(pdf_bytes) // 1024)

    return pdf_bytes


def upload_pdf_to_drive(pdf_bytes: bytes, filename: str) -> str:
    """Upload a PDF file to the target Google Drive folder.

    Returns the file ID.
    """
    import io

    from googleapiclient.http import MediaIoBaseUpload

    drive = _get_drive()
    media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
    file_meta = {
        "name": filename,
        "parents": [settings.google_drive_folder_id],
        "mimeType": "application/pdf",
    }
    uploaded = drive.files().create(body=file_meta, media_body=media, fields="id").execute()
    file_id = uploaded["id"]
    log.info("pdf_uploaded", file_id=file_id, name=filename)
    return file_id


def generate_letter(
    doc_name: str,
    replacements: dict[str, str],
    export_as_pdf: bool = True,
) -> dict:
    """Full pipeline: copy template → fill → optionally export PDF.

    Returns dict with doc_id, doc_url, and optionally pdf_file_id.
    """
    doc_id = copy_template(doc_name)
    fill_placeholders(doc_id, replacements)

    result = {
        "doc_id": doc_id,
        "doc_url": f"https://docs.google.com/document/d/{doc_id}/edit",
    }

    if export_as_pdf:
        pdf_bytes = export_pdf(doc_id)
        pdf_name = f"{doc_name}.pdf"
        pdf_id = upload_pdf_to_drive(pdf_bytes, pdf_name)
        result["pdf_file_id"] = pdf_id
        result["pdf_url"] = f"https://drive.google.com/file/d/{pdf_id}/view"

    log.info("letter_generated", **result)
    return result
