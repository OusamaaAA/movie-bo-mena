import json

import typer

from src.db import session_scope
from src.services.ingestion_service import (
    refresh_title,
    run_backfill,
    run_bom_backfill_year_range_job,
    run_daily,
    run_weekly,
)
from src.services.acquisition_lookup_runner import run_acquisition_lookup

app = typer.Typer(help="MENA Box Office Intelligence CLI")
jobs = typer.Typer(help="Ingestion jobs")
app.add_typer(jobs, name="jobs")

@app.command("acquisition-lookup")
def acquisition_lookup_cli(
    query_text: str = typer.Option(..., "--query-text", "-q", help="Title query text (EN/AR variants supported)."),
    release_year_hint: int | None = typer.Option(None, "--release-year-hint", help="Optional release year hint."),
    imdb_title_id: str | None = typer.Option(None, "--imdb-title-id", help="Optional IMDb title id (e.g. tt1234567)."),
) -> None:
    result = run_acquisition_lookup(
        query_text=query_text,
        release_year_hint=release_year_hint,
        imdb_title_id=imdb_title_id,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@jobs.command("daily")
def jobs_daily() -> None:
    with session_scope() as session:
        result = run_daily(session)
    typer.echo(json.dumps(result, indent=2, default=str))


@jobs.command("weekly")
def jobs_weekly() -> None:
    with session_scope() as session:
        result = run_weekly(session)
    typer.echo(json.dumps(result, indent=2, default=str))


@jobs.command("backfill")
def jobs_backfill(source: str = typer.Option(...), days: int = typer.Option(90)) -> None:
    with session_scope() as session:
        result = run_backfill(session, source_code=source, days=days)
    typer.echo(json.dumps(result, indent=2, default=str))


@jobs.command("bom-backfill")
def jobs_bom_backfill(
    start_year: int = typer.Option(..., help="Start year (inclusive)"),
    end_year: int = typer.Option(..., help="End year (inclusive)"),
    markets: str | None = typer.Option(default=None, help="Comma-separated market codes (e.g. AE,SA,EG). Defaults to all configured markets."),
) -> None:
    market_codes = [m.strip().upper() for m in markets.split(",") if m.strip()] if markets else None
    with session_scope() as session:
        result = run_bom_backfill_year_range_job(
            session,
            start_year=start_year,
            end_year=end_year,
            market_codes=market_codes,
        )
    typer.echo(json.dumps(result, indent=2, default=str))


@jobs.command("refresh-title")
def jobs_refresh_title(
    title: str = typer.Option(...),
    imdb_title_id: str | None = typer.Option(default=None),
) -> None:
    with session_scope() as session:
        result = refresh_title(session, title=title, imdb_title_id=imdb_title_id)
    typer.echo(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    app()

