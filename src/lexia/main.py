"""CLI entry point for LexIA."""
from __future__ import annotations

import asyncio
import json

import structlog
import typer
from rich.console import Console
from rich.table import Table

from lexia.config import settings

app = typer.Typer(name="lexia", help="LexIA — Judicial Response Letter automation")
console = Console()


def _configure_logging():
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog, settings.log_level, structlog.INFO)
        ),
    )


@app.command()
def run(
    days_back: int = typer.Option(None, "--days", "-d", help="Days lookback for cases"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Fetch cases without processing"),
    output_json: bool = typer.Option(False, "--json", help="Output results as JSON"),
):
    """Run the LexIA pipeline — fetch cases, enrich, decide macro, generate letters."""
    _configure_logging()

    from lexia.orchestrator import run_pipeline

    results = asyncio.run(run_pipeline(days_back=days_back, dry_run=dry_run))

    if output_json:
        console.print_json(json.dumps(results, ensure_ascii=False, indent=2))
        return

    table = Table(title=f"LexIA — {len(results)} caso(s) processado(s)")
    table.add_column("Case ID", style="dim", max_width=12)
    table.add_column("Tipo", style="cyan")
    table.add_column("Macro", style="green")
    table.add_column("Status", style="bold")
    table.add_column("Doc URL", style="blue", max_width=50)

    for r in results:
        table.add_row(
            r.get("case_id", "")[:12],
            r.get("tipo", r.get("macro_name", "")),
            r.get("macro", ""),
            r.get("status", ""),
            r.get("doc_url", r.get("error", "")),
        )

    console.print(table)


@app.command()
def check():
    """Verify configuration and connectivity."""
    _configure_logging()
    console.print("[bold]LexIA Configuration Check[/bold]\n")

    checks = {
        "Databricks Host": bool(settings.databricks_host),
        "Databricks Token": bool(settings.databricks_token),
        "Nu Cert Path": bool(settings.nu_cert_path),
        "Google Credentials": bool(settings.google_credentials_path),
        "Google Template ID": bool(settings.google_template_doc_id),
        "Google Drive Folder": bool(settings.google_drive_folder_id),
        "Gemini API Key": bool(settings.gemini_api_key),
    }

    for name, ok in checks.items():
        icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {icon} {name}")

    all_ok = all(checks.values())
    if all_ok:
        console.print("\n[green bold]All checks passed![/green bold]")
    else:
        console.print("\n[red bold]Some checks failed — review .env file[/red bold]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
