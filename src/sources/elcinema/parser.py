from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta

from src.sources.common import ExtractedRecord
from src.services.parser_utils import (
    MARKETS,
    compact,
    detect_elcinema_boxoffice_year_week,
    detect_elcinema_week_label,
    elcinema_native_week_period,
    find_market_code_by_country,
    html_to_text,
    iso_week_period_span,
    parse_iso_week_label,
    parse_money,
    period_span_key,
    unique_preserve_order,
)

CURRENCY_RX = r'(EGP|SAR|USD|AED|KWD|BHD|LBP|QAR|OMR|JOD)'


def parse_elcinema_search_results(html: str) -> list[dict]:
    """Extract work IDs and titles from an elCinema search results page.

    Returns a list of dicts with keys: work_id, title, year (optional).
    """
    out: list[dict] = []
    seen: set[str] = set()
    # Match work links: /en/work/NUMBER/ or /work/NUMBER/
    for m in re.finditer(r'href="(?:/en)?/work/(\d+)/?"[^>]*>([\s\S]*?)</a>', html, flags=re.I):
        work_id = m.group(1)
        if work_id in seen:
            continue
        title = html_to_text(m.group(2)).strip()
        if not title or len(title) < 2:
            continue
        # Skip non-title links (navigation, generic labels)
        if re.fullmatch(r'[\d.]+', title):
            continue
        if re.search(r'^(Playing in which Cinemas|Revenue Details|Buy tickets|More|Box Office)$', title, flags=re.I):
            continue
        seen.add(work_id)
        # Try to extract year from surrounding context
        year: int | None = None
        # Check for year in parentheses right after the title
        context = html[m.end():m.end() + 200]
        year_m = re.search(r'\(?(20\d{2})\)?', context)
        if year_m:
            year = int(year_m.group(1))
        out.append({"work_id": work_id, "title": title, "year": year})
    return out


def parse_elcinema_work_metadata(html_en: str, html_ar: str | None, work_id: str, source_url_en: str) -> dict:
    """Mirror fetchElCinemaWorkMeta_: English + optional Arabic work root pages."""
    text_en = html_to_text(html_en)
    title_en = ""
    year_s = ""
    m1 = re.search(r"#\s*(.*?)\s*\((20\d{2})\)", text_en)
    if m1:
        title_en = compact(m1.group(1))
        year_s = m1.group(2) or ""
    if not title_en:
        m2 = re.search(r"Movie\s*-\s*(.*?)\s*-\s*(20\d{2})", text_en, flags=re.I)
        if m2:
            title_en = compact(m2.group(1))
            year_s = year_s or (m2.group(2) or "")
    title_ar = ""
    if html_ar:
        text_ar = html_to_text(html_ar)
        a1 = re.search(r"#\s*([\u0600-\u06FF\s]+?)\s*\((20\d{2})\)", text_ar)
        if a1:
            title_ar = compact(a1.group(1))
            year_s = year_s or (a1.group(2) or "")
        else:
            a2 = re.search(r"فيلم\s*-\s*([\u0600-\u06FF\s]+?)\s*-\s*(20\d{2})", text_ar)
            if a2:
                title_ar = compact(a2.group(1))
                year_s = year_s or (a2.group(2) or "")
    return {
        "work_id": work_id,
        "title_en": title_en,
        "title_ar": title_ar,
        "release_year": int(year_s) if year_s else None,
        "source_url": source_url_en,
    }


_MARKET_CURRENCIES: dict[str, str] = {
    "EG": "EGP", "SA": "SAR", "AE": "AED", "KW": "KWD",
    "BH": "BHD", "QA": "QAR", "OM": "OMR", "JO": "JOD", "LB": "LBP",
}


def parse_current_chart(
    html: str,
    source_url: str,
    chart_limit: int = 100,
    country_code: str = "EG",
    fallback_date: date | None = None,
) -> list[ExtractedRecord]:
    """Mirror the effective logic of fetchElCinemaCurrentChart_ from the Apps Script.

    Strategy:
    1) discover work links from /en/work/<id>/ anchors
    2) dedupe by work id
    3) inspect the chunk after each anchor for Weekly Revenue / Total Revenue
    4) emit weekly title performance rows for the given country_code

    When the page does not embed an explicit week label, falls back to
    `fallback_date` (or today if not provided) to determine the ISO week.
    """
    period: tuple[date, date, str] | None = None
    period_label_raw: str | None = None

    # Prefer native box-office year/week when present so we never mix Thu–Wed windows
    # with ISO Mon–Sun keys for the same chart (duplicate "week N" confusion in UI/DB).
    native = detect_elcinema_boxoffice_year_week(html)
    if native:
        y, nw = native
        period = elcinema_native_week_period(y, nw)
        period_label_raw = f"{y}-EW{nw:02d}"

    if period is None:
        wk_text = detect_elcinema_week_label(html)
        parsed = parse_iso_week_label(wk_text)
        if parsed:
            s, e, _ = parsed
            period = (s, e, period_span_key(s, e))
            period_label_raw = wk_text

    # Fallback 1: try to infer week from pagination/navigation links embedded in the
    # page HTML (e.g. href="...?year=2025&week=6" and href="...?year=2025&week=8"
    # imply the current page is for week 7).  This handles markets like SA where
    # elCinema doesn't embed an explicit ISO-week label in the page text.
    if period is None:
        nav_weeks = [
            (int(ym.group(1)), int(ym.group(2)))
            for ym in re.finditer(r'year=(\d{4})[^"\'&]*&[^"\'&]*week=(\d+)', html, flags=re.I)
        ]
        if len(nav_weeks) >= 2:
            years = [y for y, _ in nav_weeks]
            weeks = [w for _, w in nav_weeks]
            unique_years = set(years)
            if len(unique_years) == 1:
                year = years[0]
                min_w, max_w = min(weeks), max(weeks)
                # If navigation shows consecutive weeks (prev/next), current is in between.
                if max_w - min_w == 2:
                    wk = min_w + 1
                    period_label_raw = f"{year}-W{wk:02d}"
                    period = iso_week_period_span(year, wk)
                elif max_w - min_w == 1:
                    # Only one nav direction; use the higher week as current
                    period_label_raw = f"{year}-W{max_w:02d}"
                    period = iso_week_period_span(year, max_w)

    # Fallback 2: use explicitly provided fallback_date (backfill calls pass this).
    # Only fall back to today as a last resort — date.today() gives the *fetch* date,
    # not the *data* date, so it must never be used for live weekly chart fetches.
    if period is None and fallback_date is not None:
        iso = fallback_date.isocalendar()
        period_label_raw = f"{iso.year}-W{iso.week:02d}"
        period = iso_week_period_span(iso.year, iso.week)
    # Work links: allow optional /en prefix and single- or double-quoted hrefs. Each chart
    # row often repeats the same work_id twice (e.g. EN + AR title); keep the *last*
    # consecutive anchor so the HTML slice reaches the "Weekly Revenue" block reliably.
    work_entries: list[tuple[str, str, int]] = []
    link_re = re.compile(
        r'href=(["\'])(?:/en)?/work/(\d+)/?\1[^>]*>([\s\S]*?)</a>',
        flags=re.I,
    )
    for m in link_re.finditer(html):
        work_id = m.group(2)
        title = html_to_text(m.group(3)).strip()
        if not title or len(title) < 2:
            continue
        if re.fullmatch(r'[\d.]+', title):
            continue
        if re.search(r'^(Playing in which Cinemas\?|Revenue Details|Buy tickets)$', title, flags=re.I):
            continue
        work_entries.append((work_id, title, m.start()))

    collapsed: list[tuple[str, str, int]] = []
    for work_id, title, idx in work_entries:
        if collapsed and collapsed[-1][0] == work_id:
            collapsed[-1] = (work_id, title, idx)
        else:
            collapsed.append((work_id, title, idx))

    deduped: list[tuple[str, str, int]] = []
    seen: set[str] = set()
    for work_id, title, idx in collapsed:
        if work_id in seen:
            continue
        seen.add(work_id)
        deduped.append((work_id, title, idx))

    rows: list[ExtractedRecord] = []
    max_items = min(chart_limit, len(deduped))
    slice_rows = deduped[:max_items]
    _chunk_tail = 12000
    for i, (work_id, title, idx) in enumerate(slice_rows):
        if i + 1 < len(deduped):
            next_idx = deduped[i + 1][2]
        else:
            next_idx = min(idx + _chunk_tail, len(html))
        chunk = html[idx:next_idx]
        chunk_text = html_to_text(chunk)

        weekly_match = re.search(rf'Weekly Revenue:\s*([\d,]+(?:\.\d+)?)\s*{CURRENCY_RX}', chunk_text, flags=re.I)
        if not weekly_match:
            continue
        total_match = re.search(rf'Total Revenue:\s*([\d,]+(?:\.\d+)?)\s*{CURRENCY_RX}', chunk_text, flags=re.I)
        rating_match = re.search(r'^(\d+(?:\.\d+)?)\s+', chunk_text)

        weekly = parse_money(weekly_match.group(1))
        total = parse_money(total_match.group(1)) if total_match else None
        currency = weekly_match.group(2).upper()

        # Currency: prefer what the page shows; fall back to market default
        market_currency = _MARKET_CURRENCIES.get(country_code.upper(), "USD")
        effective_currency = currency if currency else market_currency

        rows.append(
            ExtractedRecord(
                source_name='elCinema',
                source_url=source_url,
                source_entity_id=work_id,
                country_code=country_code,
                film_title_raw=title,
                film_title_ar_raw=None,
                release_year_hint=period[0].year if period else None,
                record_scope='title',
                record_granularity='week',
                record_semantics='title_period_gross',
                evidence_type='title_performance',
                period_label_raw=period_label_raw,
                period_start_date=period[0] if period else None,
                period_end_date=period[1] if period else None,
                period_key=period[2] if period else None,
                rank=len(rows) + 1,
                period_gross_local=weekly,
                cumulative_gross_local=total,
                currency=effective_currency,
                admissions_actual=None,
                parser_confidence=0.92 if total_match else 0.80,
                source_confidence=0.82,
                notes='Current elCinema chart record',
                raw_payload_json={
                    'work_id': work_id,
                    'title': title,
                    'chart_week': period_label_raw,
                    'weekly': weekly,
                    'total': total,
                    'rating_hint': rating_match.group(1) if rating_match else None,
                },
            )
        )
    return rows


def parse_title_boxoffice(html: str, source_url: str, work_id: str) -> list[ExtractedRecord]:
    """Mirror fetchElCinemaTitleBoxOffice_ from the Apps Script.

    The page is split into country sections (<h3>Country</h3> ...). Inside each section,
    we parse weekly rows like: 12 2025 123,456 EGP and an optional Total <amount> <currency>.
    """
    page_text = html_to_text(html)

    # Multiple fallback patterns for title extraction — elCinema's boxoffice page
    # layout varies; try the most specific pattern first, then broaden.
    title = ''
    release_year: int | None = None

    # Pattern 1 (original): "Box Office: Movie - Title - Year"
    m = re.search(r'Box Office[:\s]+Movie\s*[-–]\s*(.*?)\s*[-–]\s*(20\d{2})', page_text, flags=re.I)
    if m:
        title = compact(m.group(1))
        release_year = int(m.group(2))

    # Pattern 2: "# Title (Year)" — same heading format as the work-root metadata page
    if not title:
        m = re.search(r'#\s*(.*?)\s*\((20\d{2})\)', page_text)
        if m:
            title = compact(m.group(1))
            release_year = release_year or int(m.group(2))

    # Pattern 3: HTML <title> tag  — "Title (Year) Box Office - elCinema" or similar
    if not title:
        m = re.search(r'<title[^>]*>([\s\S]*?)</title>', html, flags=re.I)
        if m:
            raw = html_to_text(m.group(1)).strip()
            # Strip trailing site label and "Box Office" suffix to isolate film title
            raw = re.sub(r'\s*[-–|]\s*elcinema.*$', '', raw, flags=re.I)
            raw = re.sub(r'\s*Box Office.*$', '', raw, flags=re.I).strip()
            yr_m = re.search(r'\((20\d{2})\)', raw)
            if yr_m:
                title = compact(raw[:yr_m.start()])
                release_year = release_year or int(yr_m.group(1))
            elif raw and len(raw) >= 2:
                title = compact(raw)

    # Pattern 4: "Movie - Title - Year" or "Movie – Title – Year"
    if not title:
        m = re.search(r'Movie\s*[-–]\s*(.*?)\s*[-–]\s*(20\d{2})', page_text, flags=re.I)
        if m:
            title = compact(m.group(1))
            release_year = release_year or int(m.group(2))

    rows: list[ExtractedRecord] = []
    section_iter = re.finditer(r'<h3[^>]*>\s*([^<]+?)\s*</h3>([\s\S]*?)(?=<h3[^>]*>|$)', html, flags=re.I)
    for section in section_iter:
        country_label = html_to_text(section.group(1)).strip()
        market_code = find_market_code_by_country(country_label)
        if not market_code:
            continue
        section_html = section.group(2)
        section_text = html_to_text(section_html)

        week_rows: list[tuple[int, int, float, str]] = []
        for m in re.finditer(rf'(\d{{1,2}}|5[0-3])\s+(20\d{{2}})\s+([\d,]+(?:\.\d+)?)\s+{CURRENCY_RX}', section_text, flags=re.I):
            week_rows.append((int(m.group(1)), int(m.group(2)), parse_money(m.group(3)) or 0.0, m.group(4).upper()))

        total_match = re.search(rf'Total\s+([\d,]+(?:\.\d+)?)\s+{CURRENCY_RX}', section_text, flags=re.I)
        total_amount = parse_money(total_match.group(1)) if total_match else None
        total_currency = total_match.group(2).upper() if total_match else None

        if not week_rows and not total_amount:
            continue

        for week, year, amount, currency in week_rows:
            # Table week column follows elCinema native box-office index (same anchor as charts).
            start, end, key = elcinema_native_week_period(year, week)
            rows.append(
                ExtractedRecord(
                    source_name='elCinema',
                    source_url=source_url,
                    source_entity_id=f'{work_id}_{market_code}_{year}_{week}',
                    country_code=market_code,
                    film_title_raw=title,
                    film_title_ar_raw=None,
                    release_year_hint=release_year,
                    record_scope='title',
                    record_granularity='week',
                    record_semantics='title_period_gross',
                    evidence_type='title_performance',
                    period_label_raw=f'{year}-EW{week:02d}',
                    period_start_date=start,
                    period_end_date=end,
                    period_key=key,
                    rank=None,
                    period_gross_local=amount,
                    cumulative_gross_local=total_amount,
                    currency=currency,
                    admissions_actual=None,
                    parser_confidence=0.94,
                    source_confidence=0.90,
                    notes='elCinema title box office weekly record',
                    raw_payload_json={
                        'work_id': work_id,
                        'title': title,
                        'country_label': country_label,
                        'week': week,
                        'year': year,
                        'amount': amount,
                        'total_amount': total_amount,
                    },
                )
            )

        if not week_rows and total_amount is not None:
            rows.append(
                ExtractedRecord(
                    source_name='elCinema',
                    source_url=source_url,
                    source_entity_id=f'{work_id}_{market_code}_TOTAL',
                    country_code=market_code,
                    film_title_raw=title,
                    film_title_ar_raw=None,
                    release_year_hint=release_year,
                    record_scope='title',
                    record_granularity='lifetime',
                    record_semantics='title_cumulative_total',
                    evidence_type='title_performance',
                    period_label_raw='lifetime',
                    period_start_date=None,
                    period_end_date=None,
                    period_key='lifetime',
                    rank=None,
                    period_gross_local=None,
                    cumulative_gross_local=total_amount,
                    currency=total_currency,
                    admissions_actual=None,
                    parser_confidence=0.70,
                    source_confidence=0.72,
                    notes='elCinema title total only record',
                    raw_payload_json={
                        'work_id': work_id,
                        'title': title,
                        'country_label': country_label,
                        'total_amount': total_amount,
                    },
                )
            )

    return rows


def parse_title_released_markets(
    html: str,
    source_url: str,
    work_id: str,
    *,
    film_title_raw: str,
    film_title_ar_raw: str | None,
    release_year_hint: int | None,
) -> list[ExtractedRecord]:
    """Parse elCinema /released page into title-scoped evidence rows.

    Legacy Apps Script logic:
    - fetch /en/work/<id>/released
    - parse: "<Country> <d> <Month> <yyyy> <Yes|No>"
    """

    page_text = html_to_text(html)

    # Example fragments (from legacy regex):
    # Egypt 12 January 2024 Yes
    country_names = [
        "Egypt",
        "Iraq",
        "Saudi Arabia",
        "Kuwait",
        "Bahrain",
        "Oman",
        "United Arab Emirates",
        "Jordan",
        "Lebanon",
        "Syria",
        "Morocco",
    ]
    country_rx = "|".join(map(re.escape, country_names))
    rx = re.compile(rf"({country_rx})\s+(\d{{1,2}})\s+([A-Za-z]+)\s+(20\d{{2}})\s+(Yes|No)", flags=re.I)

    def _parse_release_date(day: str, month: str, year: str) -> date | None:
        m = month.lower()[:3]
        month_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        month_i = month_map.get(m)
        if not month_i:
            return None
        try:
            return date(int(year), month_i, int(day))
        except Exception:  # noqa: BLE001
            return None

    out: list[ExtractedRecord] = []
    for m in rx.finditer(page_text):
        country = m.group(1)
        day = m.group(2)
        month = m.group(3)
        year = m.group(4)
        premiere = m.group(5).lower() == "yes"

        market_code = find_market_code_by_country(country)
        if not market_code:
            continue

        release_dt = _parse_release_date(day, month, year)
        if not release_dt:
            continue

        out.append(
            ExtractedRecord(
                source_name="elCinema",
                source_url=source_url,
                source_entity_id=f"{work_id}_{market_code}",
                country_code=market_code,
                film_title_raw=film_title_raw,
                film_title_ar_raw=film_title_ar_raw,
                release_year_hint=release_year_hint,
                record_scope="title",
                record_granularity="lifetime",
                record_semantics="title_released_market",
                evidence_type="title_release_date",
                period_label_raw=release_dt.isoformat(),
                period_start_date=release_dt,
                period_end_date=None,
                period_key=f"released_{market_code}",
                rank=None,
                period_gross_local=None,
                cumulative_gross_local=None,
                currency=None,
                admissions_actual=None,
                parser_confidence=0.78,
                source_confidence=0.84,
                notes=f"elCinema released date evidence; premiere={premiere}",
                raw_payload_json={
                    "work_id": work_id,
                    "country": country,
                    "premiere": premiere,
                    "release_date": release_dt.isoformat(),
                },
            )
        )

    return out
