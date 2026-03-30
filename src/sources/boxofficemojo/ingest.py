from datetime import date

from src.sources.boxofficemojo.client import BomClient
from src.sources.boxofficemojo.parser import parse_weekend_detail_page, parse_weekend_index_page
from src.services.parser_utils import MARKETS


def run_bom_weekly() -> list:
    client = BomClient()
    rows = []
    year = date.today().year
    for market_code in MARKETS.keys():
        try:
            index_html = client.fetch_weekend_index(market_code, year)
            source_url = f"{client.base_url}/weekend/by-year/?area={market_code}&yr={year}"
            weekends = parse_weekend_index_page(index_html, source_url, market_code, year)
            if not weekends:
                continue
            latest = sorted(weekends, key=lambda x: x["weekend_code"], reverse=True)[0]
            detail_html = client.fetch_weekend_detail(market_code, latest["weekend_code"])
            detail_url = f"{client.base_url}/weekend/{latest['weekend_code']}/?area={market_code}"
            rows.extend(
                parse_weekend_detail_page(
                    detail_html,
                    detail_url,
                    market_code,
                    latest["weekend_code"],
                    period_start_date=latest.get("period_start_date"),
                    period_end_date=latest.get("period_end_date"),
                )
            )
        except Exception:  # noqa: BLE001
            continue
    return rows


def run_bom_backfill(weeks: int) -> list:
    client = BomClient()
    out = []
    current_year = date.today().year
    years_to_scan = max(1, (weeks // 52) + 1)
    for offset in range(years_to_scan):
        year = current_year - offset
        try:
            for market_code in MARKETS.keys():
                index_html = client.fetch_weekend_index(market_code, year)
                index_url = f"{client.base_url}/weekend/by-year/?area={market_code}&yr={year}"
                weekends = parse_weekend_index_page(index_html, index_url, market_code, year)
                for weekend in weekends:
                    detail_html = client.fetch_weekend_detail(market_code, weekend["weekend_code"])
                    detail_url = f"{client.base_url}/weekend/{weekend['weekend_code']}/?area={market_code}"
                    out.extend(
                        parse_weekend_detail_page(
                            detail_html,
                            detail_url,
                            market_code,
                            weekend["weekend_code"],
                            period_start_date=weekend.get("period_start_date"),
                            period_end_date=weekend.get("period_end_date"),
                        )
                    )
        except Exception:  # noqa: BLE001
            continue
    return out


def run_bom_backfill_year_range(start_year: int, end_year: int, market_codes: list[str] | None = None) -> list:
    """
    Historic BOM backfill: market-by-market, year-by-year, weekend-by-weekend.

    Mirrors the legacy Apps Script logic:
    - fetch /weekend/by-year/?area=XX
    - extract weekend codes for that market/year
    - fetch each /weekend/YYYYWNN/?area=XX detail page
    - keep title-level weekend rows
    """
    client = BomClient()
    out: list = []
    markets = market_codes or list(MARKETS.keys())
    for year in range(int(start_year), int(end_year) + 1):
        for market_code in markets:
            try:
                index_html = client.fetch_weekend_index(market_code, year)
                index_url = f"{client.base_url}/weekend/by-year/?area={market_code}&yr={year}"
                weekends = parse_weekend_index_page(index_html, index_url, market_code, year)
                for weekend in weekends:
                    detail_html = client.fetch_weekend_detail(market_code, weekend["weekend_code"])
                    detail_url = f"{client.base_url}/weekend/{weekend['weekend_code']}/?area={market_code}"
                    out.extend(
                        parse_weekend_detail_page(
                            detail_html,
                            detail_url,
                            market_code,
                            weekend["weekend_code"],
                            period_start_date=weekend.get("period_start_date"),
                            period_end_date=weekend.get("period_end_date"),
                        )
                    )
            except Exception:  # noqa: BLE001
                continue
    return out

