from __future__ import annotations

import html
import re
from datetime import date, timedelta
from typing import Iterable

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}

MARKETS = {
    'AE': {'country': 'United Arab Emirates', 'currency': 'AED'},
    'SA': {'country': 'Saudi Arabia', 'currency': 'SAR'},
    'EG': {'country': 'Egypt', 'currency': 'EGP'},
    'KW': {'country': 'Kuwait', 'currency': 'KWD'},
    'BH': {'country': 'Bahrain', 'currency': 'BHD'},
    'LB': {'country': 'Lebanon', 'currency': 'LBP'},
    'QA': {'country': 'Qatar', 'currency': 'QAR'},
    'OM': {'country': 'Oman', 'currency': 'OMR'},
    'JO': {'country': 'Jordan', 'currency': 'JOD'},
}


def html_to_text(value: str) -> str:
    text = html.unescape(value or '')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def compact(value: str) -> str:
    return re.sub(r'\s+', ' ', html.unescape(value or '')).strip()


def parse_money(value: str | None) -> float | None:
    if not value:
        return None
    m = re.search(r'([\d][\d,]*(?:\.\d+)?)', str(value))
    if not m:
        return None
    return float(m.group(1).replace(',', ''))


def parse_int_safe(value: str | None) -> int | None:
    if not value:
        return None
    m = re.search(r'\d+', str(value).replace(',', ''))
    return int(m.group(0)) if m else None


def find_market_code_by_country(label: str | None) -> str | None:
    if not label:
        return None
    normalized = compact(label).lower()
    aliases = {
        'uae': 'AE',
        'united arab emirates': 'AE',
        'saudi': 'SA',
        'saudi arabia': 'SA',
        'egypt': 'EG',
        'kuwait': 'KW',
        'bahrain': 'BH',
        'lebanon': 'LB',
        'qatar': 'QA',
        'oman': 'OM',
        'jordan': 'JO',
    }
    for k, code in aliases.items():
        if k in normalized:
            return code
    return None


def detect_elcinema_boxoffice_year_week(html: str) -> tuple[int, int] | None:
    """Parse elCinema's on-page box-office year/week (not ISO).

    Egypt and Saudi pages use the same template, e.g. ``[year 2026 week 13]`` or
    ``Egyptian Boxoffice year 2026 week 7`` — week numbers are market-specific
    (SA may be on week 7 while EG is on week 13 for the same calendar date).
    """
    m = re.search(r"\[\s*year\s*(20\d{2})\s*week\s*(\d{1,2})\s*\]", html, flags=re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(
        r"(?:Egyptian\s+)?[Bb]oxoffice\s+year\s*(20\d{2})\s*week\s*(\d{1,2})",
        html,
        flags=re.I,
    )
    if m:
        return int(m.group(1)), int(m.group(2))
    wm = re.search(
        r'<select[^>]+name=["\']week["\'][^>]*>.*?<option[^>]+selected[^>]+value="(\d+)"',
        html,
        flags=re.I | re.S,
    )
    if wm:
        y_m = re.search(
            r'<select[^>]+name=["\']year["\'][^>]*>.*?<option[^>]+selected[^>]+value="(20\d{2})"',
            html,
            flags=re.I | re.S,
        )
        if y_m:
            return int(y_m.group(1)), int(wm.group(1))
        title_y = re.search(
            r"<title[^>]*>[\s\S]{0,300}?year\s*(20\d{2})\s*week",
            html,
            flags=re.I,
        )
        if title_y:
            return int(title_y.group(1)), int(wm.group(1))
    return None


def period_span_key(start: date, end: date) -> str:
    """Canonical weekly period identity (shared across ingest + UI).

    Using an explicit ``start..end`` range avoids collisions between ISO ``YYYY-Www``
    (Mon–Sun) and elCinema's native box-office week (Thu–Wed anchor from Jan 1).
    """
    return f"{start.isoformat()}..{end.isoformat()}"


def elcinema_native_week_period(year: int, native_week: int) -> tuple[date, date, str]:
    """Map elCinema native box-office week index to Thu-anchored 7-day span.

    elCinema anchors week 1 to the box-office year starting Jan 1; consecutive weeks
    are 7-day blocks from that origin (EG/SA chart headings), not ISO week numbers.

    The returned ``period_key`` is always a **date span** — use ``period_label_raw``
    / payload if you need ``YYYY-EWww`` for display or debugging.
    """
    y = int(year)
    nw = max(1, int(native_week))
    start = date(y, 1, 1) + timedelta(days=(nw - 1) * 7)
    end = start + timedelta(days=6)
    return start, end, period_span_key(start, end)


def iso_week_period(year: int, week: int) -> tuple[date, date, str]:
    y = int(year)
    w = int(week)
    if w < 1:
        w = 1
    # Some source pages occasionally emit week 53 for years that only have 52 ISO weeks.
    # Clamp to the last valid ISO week to avoid dropping the entire title parse.
    max_iso_week = date(y, 12, 28).isocalendar().week
    if w > max_iso_week:
        w = max_iso_week
    start = date.fromisocalendar(y, w, 1)
    end = start + timedelta(days=6)
    return start, end, f"{y}-W{w:02d}"


def iso_week_period_span(year: int, week: int) -> tuple[date, date, str]:
    """Like :func:`iso_week_period` but ``period_key`` is a ``start..end`` span."""
    start, end, _ = iso_week_period(year, week)
    return start, end, period_span_key(start, end)


def parse_iso_week_label(label: str | None) -> tuple[date, date, str] | None:
    if not label:
        return None
    text = compact(label)
    m = re.search(r'(20\d{2})\s*-?\s*W(\d{1,2})', text, flags=re.I)
    if m:
        return iso_week_period(int(m.group(1)), int(m.group(2)))
    m = re.search(r'week\s*(\d{1,2})\s*(?:of)?\s*(20\d{2})', text, flags=re.I)
    if m:
        return iso_week_period(int(m.group(2)), int(m.group(1)))
    return None


def detect_elcinema_week_label(html_text: str) -> str | None:
    blob = compact(html_text)
    patterns = [
        r'(20\d{2}\s*-\s*W\d{1,2})',
        r'(W\d{1,2}\s*20\d{2})',
        r'(Week\s*\d{1,2}\s*of\s*20\d{2})',
        r'Box Office[^\n]{0,80}(\d{4}\s*-\s*W\d{1,2})',
    ]
    for pat in patterns:
        m = re.search(pat, blob, flags=re.I)
        if m:
            return compact(m.group(1))
    return None


def to_iso_date_from_long(label: str | None) -> date | None:
    if not label:
        return None
    text = compact(label).replace('.', '')
    m = re.search(r'([A-Za-z]{3,9})\s+(\d{1,2}),\s*(20\d{2})', text)
    if not m:
        return None
    month = MONTH_MAP.get(m.group(1)[:3].lower())
    if not month:
        return None
    return date(int(m.group(3)), month, int(m.group(2)))


def parse_mojo_weekend_label(label: str | None, fallback_year: int | None = None) -> tuple[date, date, str] | None:
    if not label:
        return None
    text = compact(label).replace('.', '')
    # Examples: "Jan 12-14", "Jan 31-Feb 2", "Mar 1-3, 2025"
    m = re.search(
        r'([A-Za-z]{3,9})\s+(\d{1,2})\s*[-–]\s*(?:([A-Za-z]{3,9})\s*)?(\d{1,2})(?:,\s*(20\d{2}))?',
        text,
        flags=re.I,
    )
    if not m:
        return None
    start_month = MONTH_MAP.get(m.group(1)[:3].lower())
    end_month = MONTH_MAP.get((m.group(3) or m.group(1))[:3].lower())
    explicit_year = m.group(5)
    if explicit_year:
        year = int(explicit_year)
    elif fallback_year is not None:
        year = int(fallback_year)
    else:
        # Do not guess from "today" — callers must pass the index/weekend year or label must
        # include the year (e.g. "Mar 1-3, 2025").
        return None
    if not start_month or not end_month:
        return None
    start = date(year, start_month, int(m.group(2)))
    end_year = year + 1 if end_month < start_month else year
    end = date(end_year, end_month, int(m.group(4)))
    return start, end, f'{start.isoformat()}..{end.isoformat()}'


def unique_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out
