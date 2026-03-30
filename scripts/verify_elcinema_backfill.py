#!/usr/bin/env python3
"""Mirror production elCinema backfill + _to_raw: no DB required.

Usage:
  python scripts/verify_elcinema_backfill.py           # days=90, network fetch
  python scripts/verify_elcinema_backfill.py 200       # days=200 → reaches deeper 2025

Prints: calendar coverage, ISO weeks fetched, parser row counts, stable raw IDs (ingest-safe).
"""
from __future__ import annotations

import sys
from datetime import date, timedelta

from src.services.ingestion_service import _to_raw
from src.sources.elcinema.ingest import run_elcinema_backfill


def _simulate_iso_plan(days: int) -> tuple[date, date, list[tuple[int, int, date]]]:
    today = date.today()
    start = today - timedelta(days=max(0, int(days)))
    seen_iso: set[tuple[int, int]] = set()
    plan: list[tuple[int, int, date]] = []
    d = today
    while d >= start:
        iso = d.isocalendar()
        key = (int(iso.year), int(iso.week))
        if key not in seen_iso:
            seen_iso.add(key)
            plan.append((key[0], key[1], d))
        d -= timedelta(days=7)
    return start, today, plan


def main() -> None:
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 90
    start, today, plan = _simulate_iso_plan(days)
    print(f"Calendar window: {start} .. {today} ({days} days)")
    print(f"Distinct ISO chart weeks to fetch (EG+SA each): {len(plan)}")
    print(f"First week in plan: {plan[0]!r}  Last week in plan: {plan[-1]!r}")
    print("--- fetching (same as Data Admin backfill) ---")
    records = run_elcinema_backfill(days)
    chart = [r for r in records if getattr(r, "record_granularity", "") == "week" and getattr(r, "record_scope", "") == "title"]
    title_hist = len(records) - len(chart)
    print(f"ExtractedRecord total: {len(records)}  (chart rows {len(chart)}, title pages ~{title_hist})")
    # Same path as ingest_source (without DB / matcher)
    from datetime import datetime, timezone

    raw_ids = []
    for r in chart[:5000]:
        raw = _to_raw(
            run_id="verify-run",
            record=r,
            fetched_at=datetime.now(timezone.utc),
            ticket_price_by_market_code=None,
        )
        raw_ids.append(str(raw.id))
    print(f"_to_raw chart rows sampled: {len(raw_ids)}  unique IDs: {len(set(raw_ids))}")
    by_mkt: dict[str, int] = {}
    for r in chart:
        by_mkt[r.country_code or "?"] = by_mkt.get(r.country_code or "?", 0) + 1
    print("Chart rows by market:", dict(sorted(by_mkt.items())))


if __name__ == "__main__":
    main()
