"""Update Google Sheets column X with links to original ofício PDFs.

Reads process numbers from column A, searches Google Drive via Apps Script,
and writes the Drive link (or "não encontrado") to column X.

Usage:
    python scripts/update_oficio_links.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import gspread
import httpx
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lexia.config import settings

SPREADSHEET_ID = os.environ.get("LEXIA_SPREADSHEET_ID", "")
SHEET_NAME = "Relatorio_final"
APPS_SCRIPT_URL = settings.apps_script_url
_folder_ids_raw = os.environ.get("LEXIA_SEARCH_FOLDER_IDS", "")
SEARCH_FOLDER_IDS = _folder_ids_raw.split(",") if _folder_ids_raw else []
OFICIO_COL = "X"


def _call_apps_script_search(processes: list[str]) -> dict:
    """POST to Apps Script search endpoint and follow the 302 redirect."""
    payload = {
        "action": "search",
        "processes": processes,
        "folderIds": SEARCH_FOLDER_IDS,
    }
    with httpx.Client(timeout=120, follow_redirects=False) as client:
        resp = client.post(
            APPS_SCRIPT_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 302:
            redirect_url = resp.headers["location"]
            resp = client.get(redirect_url)

        return resp.json()


def main():
    sa_path = settings.google_service_account_path
    if not sa_path or not Path(sa_path).exists():
        print("Google Service Account não configurado.")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_NAME)

    all_values = ws.get_all_values()
    if not all_values:
        print("Planilha vazia.")
        return

    header = all_values[0]
    print(f"Header: {header[:5]}... ({len(header)} colunas)")

    processes = []
    for _i, row in enumerate(all_values[1:], start=2):
        if row and row[0].strip():
            processes.append(row[0].strip())

    if not processes:
        print("Nenhum processo encontrado na coluna A.")
        return

    print(f"\nBuscando ofícios para {len(processes)} processos via Apps Script...")
    results = _call_apps_script_search(processes)

    print("\nResultados da busca:")
    for proc, info in results.items():
        if info:
            print(f"  ✓ {proc} → {info['name']} ({info['folder']})")
        else:
            print(f"  ✗ {proc} → não encontrado")

    col_x_header = "oficio_pdf_link"
    updates = [[col_x_header]]
    for row in all_values[1:]:
        proc = row[0].strip() if row else ""
        info = results.get(proc)
        if info:
            updates.append([info["url"]])
        else:
            updates.append(["não encontrado"])

    ws.update(range_name=f"{OFICIO_COL}1", values=updates)
    ws.format(f"{OFICIO_COL}1", {"textFormat": {"bold": True}})

    print(f"\n✓ Coluna {OFICIO_COL} atualizada com {len(updates) - 1} links.")
    print(f"  Planilha: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")


if __name__ == "__main__":
    main()
