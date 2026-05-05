"""Shadow-mode comparison report for LexIA's deterministic engine.

Reads the ``Relatorio_final`` Google Sheet (or a local CSV) populated in
``LEXIA_DECISION_MODE=shadow`` and prints a divergence report comparing
the LLM output (authoritative in shadow mode) against the deterministic
engine output stored in the ``det_*`` columns.

Usage:
    # From the spreadsheet configured in .env (default)
    python scripts/compare_shadow.py

    # From a local CSV export (faster iteration)
    python scripts/compare_shadow.py --csv path/to/export.csv

    # Top-N divergent texts to display
    python scripts/compare_shadow.py --top 20

The report includes:
    1. % de macros que bateram (alvo: >= 98%).
    2. Distribuição por confidence (HIGH / LOW / N/A).
    3. Distribuição por decision_source (deterministic / llm / llm_fallback).
    4. Matriz de confusão de macros (LLM vs determinístico) quando divergem.
    5. Top-N maiores divergências de texto (similaridade < 0.95) p/ revisão.

Exit code:
    0 — relatório gerado.
    1 — falha lendo a fonte de dados.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

REQUIRED_COLS = (
    "lexia_id",
    "id_oficio",
    "tipo_oficio",
    "llm_id_macro",
    "llm_macro_aplicada",
    "llm_texto_resposta",
    "det_id_macro",
    "det_macro_aplicada",
    "det_texto_resposta",
    "det_decision_source",
    "det_match_macro",
    "det_match_text_similarity",
    "det_confidence",
    "det_decision_reason",
)


def _read_sheet() -> tuple[list[str], list[list[str]]]:
    """Fetch all rows from the configured Google Sheet."""
    import gspread
    from google.oauth2.service_account import Credentials

    from lexia.config import settings

    if not settings.spreadsheet_id:
        raise RuntimeError("LEXIA_SPREADSHEET_ID não configurado.")
    if not settings.google_service_account_path:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_PATH não configurado.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        settings.google_service_account_path, scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(settings.spreadsheet_id)
    ws = sh.worksheet("Relatorio_final")
    values = ws.get_all_values()
    if not values:
        return [], []
    return values[0], values[1:]


def _read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    return rows[0], rows[1:]


def _split_pipe(value: str) -> list[str]:
    """Split a ``" | "``-joined cell into per-investigated values."""
    return [s.strip() for s in (value or "").split("|")]


def _explode(rows: Iterable[list[str]], header: list[str]) -> list[dict[str, str]]:
    """Explode pipe-joined rows into per-investigated dicts."""
    idx = {col: header.index(col) for col in header}
    out: list[dict[str, str]] = []
    for row in rows:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))

        llm_macros = _split_pipe(row[idx["llm_id_macro"]])
        det_macros = _split_pipe(row[idx.get("det_id_macro", -1)] if "det_id_macro" in idx else "")
        n = max(len(llm_macros), len(det_macros), 1)

        for i in range(n):
            out.append(
                {
                    "lexia_id": row[idx["lexia_id"]] if "lexia_id" in idx else "",
                    "id_oficio": row[idx["id_oficio"]] if "id_oficio" in idx else "",
                    "tipo_oficio": row[idx["tipo_oficio"]] if "tipo_oficio" in idx else "",
                    "llm_id_macro": (llm_macros[i] if i < len(llm_macros) else ""),
                    "llm_texto_resposta": (
                        _split_pipe(row[idx["llm_texto_resposta"]])[i]
                        if i < len(_split_pipe(row[idx["llm_texto_resposta"]]))
                        else ""
                    ),
                    "det_id_macro": (det_macros[i] if i < len(det_macros) else ""),
                    "det_texto_resposta": (
                        _split_pipe(row[idx["det_texto_resposta"]])[i]
                        if "det_texto_resposta" in idx
                        and i < len(_split_pipe(row[idx["det_texto_resposta"]]))
                        else ""
                    ),
                    "det_decision_source": (
                        _split_pipe(row[idx["det_decision_source"]])[i]
                        if "det_decision_source" in idx
                        and i < len(_split_pipe(row[idx["det_decision_source"]]))
                        else "skipped"
                    ),
                    "det_confidence": (
                        _split_pipe(row[idx["det_confidence"]])[i]
                        if "det_confidence" in idx
                        and i < len(_split_pipe(row[idx["det_confidence"]]))
                        else "N/A"
                    ),
                    "det_decision_reason": (
                        _split_pipe(row[idx["det_decision_reason"]])[i]
                        if "det_decision_reason" in idx
                        and i < len(_split_pipe(row[idx["det_decision_reason"]]))
                        else ""
                    ),
                }
            )
    return out


def _similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher

    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _print_section(title: str) -> None:
    bar = "=" * 70
    print(f"\n{bar}\n{title}\n{bar}")


def _build_report(records: list[dict[str, str]], top_n: int) -> int:
    """Print the divergence report. Returns exit code."""
    if not records:
        print("Nenhuma linha encontrada na fonte de dados.")
        return 1

    runnable = [
        r for r in records
        if r["det_decision_source"] not in ("skipped", "", "N/A")
        and r["det_id_macro"]
    ]
    skipped_count = len(records) - len(runnable)

    _print_section("LexIA — Shadow Mode Comparison Report")
    print(f"Total de linhas-investigado:        {len(records)}")
    print(f"  - Determinístico aplicável:       {len(runnable)}")
    print(f"  - Pulado (mode=llm ou erro):      {skipped_count}")

    if not runnable:
        print("\nNenhum registro com determinístico aplicável. Nada a comparar.")
        return 0

    matched = sum(1 for r in runnable if r["llm_id_macro"] == r["det_id_macro"])
    pct = matched / len(runnable) * 100
    print(f"\nMacros idênticas (det == LLM):       {matched}/{len(runnable)} ({pct:.1f}%)")
    target_emoji = "✓" if pct >= 98 else "✗"
    print(f"  Meta >= 98%:                       {target_emoji}")

    _print_section("Distribuição por confidence")
    conf_dist = Counter(r["det_confidence"] for r in runnable)
    for conf, n in conf_dist.most_common():
        print(f"  {conf:<8} {n:>5} ({n / len(runnable) * 100:.1f}%)")

    _print_section("Distribuição por decision_source")
    source_dist = Counter(r["det_decision_source"] for r in runnable)
    for src, n in source_dist.most_common():
        print(f"  {src:<20} {n:>5} ({n / len(runnable) * 100:.1f}%)")

    _print_section("Matriz LLM vs Determinístico (apenas divergências)")
    diverge: dict[tuple[str, str], int] = defaultdict(int)
    for r in runnable:
        if r["llm_id_macro"] != r["det_id_macro"]:
            diverge[(r["llm_id_macro"], r["det_id_macro"])] += 1
    if not diverge:
        print("  ✓ Nenhuma divergência de macro. Engine determinístico bateu 100%.")
    else:
        print(f"  {'LLM':<10} {'DET':<10} {'count':>5}")
        for (llm_m, det_m), n in sorted(diverge.items(), key=lambda x: -x[1]):
            print(f"  {llm_m:<10} {det_m:<10} {n:>5}")

    _print_section(f"Top-{top_n} divergências de texto (similarity < 0.95)")
    text_diffs = [
        (
            _similarity(r["llm_texto_resposta"], r["det_texto_resposta"]),
            r,
        )
        for r in runnable
        if r["det_texto_resposta"]
    ]
    text_diffs.sort(key=lambda x: x[0])
    shown = 0
    for sim, r in text_diffs:
        if sim >= 0.95:
            break
        if shown >= top_n:
            break
        shown += 1
        print(f"\n--- [{shown}] similarity={sim:.3f} ---")
        print(f"  lexia_id:  {r['lexia_id']}")
        print(f"  id_oficio: {r['id_oficio'][:24]}...  tipo: {r['tipo_oficio']}")
        print(f"  macro: LLM={r['llm_id_macro']!r:<6}  DET={r['det_id_macro']!r}")
        print(f"  reason: {r['det_decision_reason']}")
        print(f"  LLM : {r['llm_texto_resposta'][:200]}...")
        print(f"  DET : {r['det_texto_resposta'][:200]}...")
    if shown == 0:
        print("  ✓ Nenhum texto com similaridade < 0.95. Engine bateu textualmente.")

    _print_section("Resumo")
    print(f"  Match de macros: {pct:.1f}%")
    print(f"  Casos para revisar (texto < 0.95): {shown}")
    print(f"  Casos LOW confidence (iriam pro fallback em hybrid): "
          f"{conf_dist.get('LOW', 0)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        help="Read from a local CSV instead of fetching the Google Sheet.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top text divergences to show (default: 10).",
    )
    args = parser.parse_args()

    try:
        if args.csv:
            header, rows = _read_csv(args.csv)
        else:
            header, rows = _read_sheet()
    except Exception as e:
        print(f"[ERRO] Falha ao ler dados: {e}", file=sys.stderr)
        return 1

    missing = [c for c in REQUIRED_COLS if c not in header]
    if missing:
        print(
            f"[AVISO] Colunas faltando no header (rodou em modo legado?): {missing}",
            file=sys.stderr,
        )

    records = _explode(rows, header)
    return _build_report(records, args.top)


if __name__ == "__main__":
    sys.exit(main())
