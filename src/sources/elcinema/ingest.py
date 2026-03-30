from datetime import date, timedelta

from src.sources.elcinema.client import ElCinemaClient
from src.sources.elcinema.parser import parse_current_chart, parse_title_boxoffice, parse_title_released_markets


def run_elcinema_title_released_markets(
    *,
    work_id: str,
    film_title_raw: str,
    film_title_ar_raw: str | None,
    release_year_hint: int | None,
) -> list:
    """Fetch and parse elCinema released page for a specific work."""

    client = ElCinemaClient()
    html = client.fetch_title_released(work_id)
    source_url = f"{client.base_url}/en/work/{work_id}/released"
    return parse_title_released_markets(
        html,
        source_url,
        work_id,
        film_title_raw=film_title_raw,
        film_title_ar_raw=film_title_ar_raw,
        release_year_hint=release_year_hint,
    )


def run_elcinema_weekly(markets: list[str] | None = None) -> list:
    """
    Fetch the current weekly box office chart from elCinema for each market.
    Defaults to Egypt + Saudi Arabia. More markets can be added as elCinema expands.
    """
    if markets is None:
        markets = ["EG", "SA"]

    client = ElCinemaClient()
    out = []
    for market_code in markets:
        try:
            html = client.fetch_boxoffice_chart_market(market_code)
            if not html or len(html) < 500:
                continue
            # Build a clean source URL reflecting the market fetched
            source_url = f"{client.base_url}/en/boxoffice/{market_code}/"
            records = parse_current_chart(html, source_url, country_code=market_code)
            out.extend(records)
        except Exception:  # noqa: BLE001
            continue
    return out


def run_elcinema_backfill(
    days: int,
    *,
    markets: list[str] | None = None,
) -> list:
    """Backfill elCinema chart snapshots for the calendar window ``[today - days, today]``.

    Walks **distinct ISO weeks** in that window (step 7 days backward until before the start
    date). This fixes the old ``range(ceil(days/7))`` behaviour, which only reached
    ``(ceil(days/7) - 1) * 7`` days back — e.g. 90 days requested but ~84 days covered.

    Fetches each of ``markets`` (default Egypt + Saudi). **Note:** weeks in 2025 are not
    touched unless ``days`` is large enough (e.g. from Mar 2026 to 2025 W41 needs ~170+ days).
    """
    if markets is None:
        markets = ["EG", "SA"]

    client = ElCinemaClient()
    out: list = []
    seen_work_ids: set[str] = set()
    today = date.today()
    start = today - timedelta(days=max(0, int(days)))
    seen_iso: set[tuple[int, int]] = set()
    d = today
    while d >= start:
        iso = d.isocalendar()
        iso_key = (int(iso.year), int(iso.week))
        if iso_key not in seen_iso:
            seen_iso.add(iso_key)
            for market_code in markets:
                mc = (market_code or "").strip().upper() or "EG"
                try:
                    html = client.fetch_boxoffice_chart_week(iso.year, iso.week, mc)
                    if not html or len(html) < 500:
                        continue
                    source_url = (
                        f"{client.base_url}/en/boxoffice/{mc}/"
                        f"?year={iso.year}&week={iso.week}"
                    )
                    rows = parse_current_chart(
                        html,
                        source_url,
                        country_code=mc,
                        fallback_date=d,
                    )
                    out.extend(rows)
                    for row in rows:
                        if row.source_entity_id:
                            seen_work_ids.add(row.source_entity_id)
                except Exception:  # noqa: BLE001
                    continue
        d -= timedelta(days=7)
    # Pull title-level history pages for discovered works.
    for work_id in list(seen_work_ids)[:400]:
        try:
            html = client.fetch_title_boxoffice(work_id)
            out.extend(parse_title_boxoffice(html, f"{client.base_url}/en/work/{work_id}/boxoffice", work_id))
        except Exception:  # noqa: BLE001
            continue
    return out

