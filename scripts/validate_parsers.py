from datetime import date
import sys
from pathlib import Path

# Allow running as: `python scripts/validate_parsers.py ...` from project root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import typer

from src.sources.boxofficemojo.client import BomClient
from src.sources.boxofficemojo.parser import parse_weekend_detail_page, parse_weekend_index_page
from src.sources.elcinema.client import ElCinemaClient
from src.sources.elcinema.parser import parse_current_chart
from src.sources.filmyard.client import FilmyardClient
from src.sources.filmyard.parser import parse_daily_egypt
from src.services.parser_utils import MARKETS

app = typer.Typer(help="Quick parser sanity checks")


@app.command("filmyard")
def validate_filmyard() -> None:
    client = FilmyardClient()
    html = client.fetch_daily_egypt_page()

    with open("filmyard_debug.html", "w", encoding="utf-8") as f:
        f.write(html)

    typer.echo(f"html_length={len(html)}")
    typer.echo(f"has_boxoffice={'boxoffice' in html.lower()}")
    typer.echo(f"has_egp={'egp' in html.lower()}")
    typer.echo(f"has_table={'<table' in html.lower()}")

    from src.sources.filmyard.parser import _extract_inertia_payload, _find_candidate_rows
    payload = _extract_inertia_payload(html)
    typer.echo(f"has_payload={payload is not None}")
    if payload:
        props = payload.get("props", {})
        typer.echo(f"props_keys={list(props.keys())[:30]}")
        candidates = _find_candidate_rows(props)
        typer.echo(f"candidate_lists={len(candidates)}")
        if candidates:
            typer.echo(f"first_candidate_keys={list(candidates[0][0].keys()) if candidates[0] else []}")

    rows = parse_daily_egypt(html, f"{client.base_url}/boxoffice")
    typer.echo(f"filmyard_rows={len(rows)}")
    if rows:
        typer.echo(str(rows[0]))


@app.command("elcinema")
def validate_elcinema() -> None:
    client = ElCinemaClient()
    html = client.fetch_boxoffice_chart()
    rows = parse_current_chart(html, f"{client.base_url}/en/boxoffice")
    typer.echo(f"elcinema_rows={len(rows)}")
    if rows:
        typer.echo(str(rows[0]))


@app.command("bom")
def validate_bom() -> None:
    client = BomClient()
    year = date.today().year
    market = "AE"

    index_html = client.fetch_weekend_index(market, year)
    index_rows = parse_weekend_index_page(
        index_html,
        f"{client.base_url}/weekend/by-year/?area={market}&yr={year}",
        market,
        year,
    )
    typer.echo(f"bom_index_rows={len(index_rows)}")
    if not index_rows:
        return

    latest = sorted(index_rows, key=lambda x: x["weekend_code"], reverse=True)[0]
    typer.echo(f"latest_weekend={latest['weekend_code']}")

    detail_html = client.fetch_weekend_detail(market, latest["weekend_code"])
    detail_rows = parse_weekend_detail_page(
        detail_html,
        f"{client.base_url}/weekend/{latest['weekend_code']}/?area={market}",
        market,
        latest["weekend_code"],
    )
    typer.echo(f"bom_detail_rows={len(detail_rows)}")
    if detail_rows:
        typer.echo(str(detail_rows[0]))


if __name__ == "__main__":
    app()

