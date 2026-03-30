"""Human-readable period labels for Film Report / Live Fetch (period_key + reconciled rows)."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date


def format_period(pk: str) -> str:
    if not pk:
        return "-"
    if pk == "lifetime":
        return "All-time"
    m = re.match(r"(\d{4})-W(\d+)$", pk)
    if m:
        return f"W{int(m.group(2))} ({m.group(1)})"
    m = re.match(r"(\d{4})-EW(\d+)$", pk)
    if m:
        return f"EW{int(m.group(2)):02d} ({m.group(1)})"
    m = re.match(r"(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})$", pk)
    if m:
        d = date.fromisoformat(m.group(1))
        return f"{d.strftime('%b %d')} '{str(d.year)[2:]}"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", pk)
    if m:
        d = date.fromisoformat(pk)
        return f"{d.strftime('%b %d')} '{str(d.year)[2:]}"
    return pk


def format_period_row(row: dict) -> str:
    """Prefer ``period_start_date`` (from reconciled) so labels match DB dates, not a bad ``period_key``."""
    ps = row.get("period_start_date")
    if ps:
        try:
            d = date.fromisoformat(ps[:10]) if isinstance(ps, str) else ps
            return f"{d.strftime('%b %d')} '{str(d.year)[2:]}"
        except (ValueError, TypeError):
            pass
    return format_period(str(row.get("period_key") or ""))


def dedupe_bom_weekend_rows(rows: list[dict]) -> list[dict]:
    """Collapse duplicate Box Office Mojo weekend rows that only differ by year (bad ingest + repair).

    Groups by (market, calendar month/day of weekend start, rounded gross) and keeps the row
    whose ``period_start_date`` year is **minimum** (typically the correct 2025 vs erroneous 2026).
    """
    bom_weekend: list[dict] = []
    other: list[dict] = []
    for r in rows:
        src = (r.get("source") or "").strip()
        gran = (r.get("granularity") or "").lower()
        if src == "Box Office Mojo" and gran == "weekend":
            bom_weekend.append(r)
        else:
            other.append(r)

    def parse_start(r: dict) -> date | None:
        ps = r.get("period_start_date")
        if ps:
            try:
                return date.fromisoformat(ps[:10]) if isinstance(ps, str) else ps
            except (ValueError, TypeError):
                pass
        pk = str(r.get("period_key") or "")
        m = re.match(r"(\d{4}-\d{2}-\d{2})\.\.", pk)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                return None
        return None

    buckets: dict[tuple[str, int, int, float], list[dict]] = defaultdict(list)
    loose: list[dict] = []
    for r in bom_weekend:
        d0 = parse_start(r)
        if d0 is None:
            loose.append(r)
            continue
        code = (r.get("country_code") or "").upper()
        g = round(float(r.get("period_gross_local") or 0), 2)
        buckets[(code, d0.month, d0.day, g)].append(r)

    picked: list[dict] = []
    for group in buckets.values():
        if len(group) == 1:
            picked.append(group[0])
        else:
            picked.append(min(group, key=lambda r: parse_start(r) or date.max))
    return other + loose + picked
