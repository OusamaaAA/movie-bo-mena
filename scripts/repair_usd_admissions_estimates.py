"""
Recompute persisted admissions_estimated for UAE / Saudi rows where gross is in USD
but estimates were stored using the old (wrong) AED/SAR ticket divisor on raw USD amounts.

Updates:
  - raw_evidence.admissions_estimated
  - normalized_evidence.admissions_estimated (rows linked to those raws)
  - rebuilds reconciled_evidence for touched films

Skips rows with admissions_actual set (estimates are not stored then anyway).

Usage (repo root):
  python scripts/repair_usd_admissions_estimates.py --dry-run
  python scripts/repair_usd_admissions_estimates.py
  python scripts/repair_usd_admissions_estimates.py --markets AE,SA
"""
from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import session_scope
from src.models import MarketReference, NormalizedEvidence, RawEvidence
from src.services.admissions_estimation import admissions_estimated_for_evidence
from src.services.ingestion_service import rebuild_reconciled_for_film


def _ticket_prices_from_session(session: Session) -> dict[str, Decimal | float]:
    return {
        str(r.market_code): r.value_num
        for r in session.execute(
            select(MarketReference).where(MarketReference.reference_type == "ticket_price")
        ).scalars().all()
        if r.value_num is not None
    }


def _recompute_raw(raw: RawEvidence, ticket_price_by_market_code: dict[str, Decimal | float]) -> Decimal | None:
    return admissions_estimated_for_evidence(
        admissions_actual=raw.admissions_actual,
        record_semantics=raw.record_semantics,
        period_gross_local=raw.period_gross_local,
        cumulative_gross_local=raw.cumulative_gross_local,
        country_code=raw.country_code,
        currency=raw.currency,
        ticket_price_by_market_code=ticket_price_by_market_code,
    )


def run_repair(
    *,
    session: Session,
    markets: list[str],
    dry_run: bool,
) -> dict:
    markets_u = [m.strip().upper() for m in markets if m.strip()]
    ticket_map = _ticket_prices_from_session(session)

    raws = list(
        session.execute(
            select(RawEvidence).where(
                RawEvidence.country_code.in_(markets_u),
                RawEvidence.admissions_actual.is_(None),
                RawEvidence.currency.isnot(None),
            )
        ).scalars().all()
    )
    # USD only (BOM international); case-insensitive
    raws = [r for r in raws if (r.currency or "").strip().upper() == "USD"]

    updates: list[tuple[str, str | None, str | None, str | None]] = []
    film_ids: set[str] = set()

    for raw in raws:
        new_est = _recompute_raw(raw, ticket_map)
        old_est = raw.admissions_estimated
        old_cmp = Decimal(str(old_est)) if old_est is not None else None
        if new_est == old_cmp:
            continue
        updates.append((raw.id, str(old_est) if old_est is not None else None, str(new_est) if new_est is not None else None, raw.country_code))
        if not dry_run:
            raw.admissions_estimated = new_est
        norms = list(
            session.execute(
                select(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == raw.id)
            ).scalars().all()
        )
        for n in norms:
            if not dry_run:
                n.admissions_estimated = new_est
            if n.film_id:
                film_ids.add(str(n.film_id))

    rebuilt: dict[str, int] = {}
    if not dry_run and updates:
        session.flush()
        for fid in sorted(film_ids):
            rebuilt[fid] = rebuild_reconciled_for_film(session, fid)

    return {
        "dry_run": dry_run,
        "markets": markets_u,
        "usd_raw_candidates": len(raws),
        "rows_updated": len(updates),
        "film_ids": sorted(film_ids),
        "reconciled_rebuilt": rebuilt,
        "sample": updates[:24],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--markets",
        default="AE,SA",
        help="Comma-separated country codes (default: AE,SA)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Report changes without committing")
    args = ap.parse_args()
    markets = [x.strip() for x in args.markets.split(",") if x.strip()]
    if not markets:
        markets = ["AE", "SA"]
    with session_scope() as session:
        out = run_repair(session=session, markets=markets, dry_run=args.dry_run)
    for k, v in out.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
