"""
Remove Box Office Mojo *release-page* raw rows that were tagged country_code=AE
but whose BOM release URL is not the United Arab Emirates release.

This fixes data ingested before the parser stopped using the multi-territory tab
strip for market detection (see parse_release_page_evidence).

Safe-by-default:
  - Only touches RawEvidence with source_name=Box Office Mojo, country_code=AE,
    and source_url containing /release/rl (title/weekend index URLs are out of scope).
  - Re-fetches each distinct release URL once; uses the same Grosses-summary
    territory parser as production. If fetch fails or territory cannot be read, the
    rows are left unchanged.
  - Keeps rows when the summary territory is UAE (including string fallback).
  - Deletes rows only when we positively identify a non-UAE territory label.
  - Rebuilds reconciled evidence only for film_ids affected by deletions.

Usage (from repo root):
  python scripts/cleanup_bom_ae_mislabeled_releases.py --dry-run
  python scripts/cleanup_bom_ae_mislabeled_releases.py
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from src.db import session_scope
from src.models import NormalizedEvidence, RawEvidence
from src.services.ingestion_service import rebuild_reconciled_for_film
from src.services.parser_utils import find_market_code_by_country, html_to_text
from src.sources.boxofficemojo.client import BomClient
from src.sources.boxofficemojo.parser import (
    _parse_bom_release_territory_summary,
    coerce_bom_release_weekend_url,
)

RELEASE_RL_RX = re.compile(r"/release/(rl\d+)", re.I)


def _canonical_release_weekend_url(source_url: str | None) -> str | None:
    if not source_url:
        return None
    return coerce_bom_release_weekend_url(source_url.strip())


def _is_uae_territory_label(territory: str) -> bool:
    t = (territory or "").lower().strip()
    return "united arab emirates" in t or t == "uae"


def _classify_release_html(html: str) -> str:
    """
    Return:
      'uae' — summary territory is UAE (keep AE-tagged rows)
      'foreign' — summary territory is not UAE (delete AE-tagged rows)
      'unknown' — do not delete (layout change, empty parse, etc.)
    """
    text = html_to_text(html)
    territory, _gross = _parse_bom_release_territory_summary(text)
    if not territory or len(territory.strip()) < 2:
        return "unknown"
    code = find_market_code_by_country(territory)
    if code == "AE" or _is_uae_territory_label(territory):
        return "uae"
    # Mapped MENA or unmapped (UK, AU, …): not the UAE release page
    return "foreign"


def _group_raw_ids_by_release_url(session: Session) -> dict[str, list[str]]:
    """Map canonical weekend release URL -> list of raw_evidence ids."""
    rows = session.execute(
        select(RawEvidence.id, RawEvidence.source_url).where(
            RawEvidence.source_name == "Box Office Mojo",
            RawEvidence.country_code == "AE",
            RawEvidence.source_url.isnot(None),
            RawEvidence.source_url.like("%/release/rl%"),
        )
    ).all()
    groups: dict[str, list[str]] = defaultdict(list)
    for rid, surl in rows:
        canon = _canonical_release_weekend_url(surl)
        if not canon or not RELEASE_RL_RX.search(canon):
            continue
        groups[canon].append(str(rid))
    return dict(groups)


def _film_ids_for_raw_ids(session: Session, raw_ids: list[str]) -> set[str]:
    if not raw_ids:
        return set()
    rows = session.execute(
        select(NormalizedEvidence.film_id).where(
            NormalizedEvidence.raw_evidence_id.in_(raw_ids),
            NormalizedEvidence.film_id.isnot(None),
        )
    ).all()
    return {str(f) for (f,) in rows if f}


def run_cleanup(*, dry_run: bool, sleep_s: float, verbose: bool) -> dict:
    client = BomClient()
    to_delete: list[str] = []
    plan: list[dict] = []

    with session_scope() as session:
        groups = _group_raw_ids_by_release_url(session)
        for canon_url in sorted(groups.keys()):
            raw_ids = groups[canon_url]
            try:
                html = client.fetch_release_page(canon_url)
            except Exception as exc:  # noqa: BLE001
                plan.append(
                    {
                        "release_url": canon_url,
                        "raw_count": len(raw_ids),
                        "decision": "skip_fetch_error",
                        "detail": str(exc)[:200],
                    }
                )
                if sleep_s > 0:
                    time.sleep(sleep_s)
                continue
            try:
                label = _classify_release_html(html)
                if label == "unknown":
                    plan.append(
                        {
                            "release_url": canon_url,
                            "raw_count": len(raw_ids),
                            "decision": "skip_unknown_territory",
                        }
                    )
                elif label == "uae":
                    plan.append(
                        {
                            "release_url": canon_url,
                            "raw_count": len(raw_ids),
                            "decision": "keep_uae",
                        }
                    )
                else:
                    # foreign — mislabeled AE rows for this URL
                    plan.append(
                        {
                            "release_url": canon_url,
                            "raw_count": len(raw_ids),
                            "decision": "delete_mislabeled_ae",
                            "raw_ids": raw_ids,
                        }
                    )
                    to_delete.extend(raw_ids)
            finally:
                if sleep_s > 0:
                    time.sleep(sleep_s)

        affected_films: set[str] = set()
        if to_delete:
            affected_films = _film_ids_for_raw_ids(session, to_delete)

        if verbose:
            for row in plan:
                print(row)

        if dry_run:
            return {
                "dry_run": True,
                "release_groups_scanned": len(groups),
                "raw_rows_to_delete": len(to_delete),
                "films_to_rebuild": sorted(affected_films),
                "plan": plan,
            }

        deleted = 0
        if to_delete:
            res = session.execute(delete(RawEvidence).where(RawEvidence.id.in_(to_delete)))
            deleted = int(res.rowcount or 0)
            session.flush()

        rebuilt: dict[str, int] = {}
        for fid in sorted(affected_films):
            rebuilt[fid] = rebuild_reconciled_for_film(session, fid)

        return {
            "dry_run": False,
            "release_groups_scanned": len(groups),
            "raw_rows_deleted": deleted,
            "films_reconciled": rebuilt,
            "plan_summary": {
                "keep_uae": sum(1 for p in plan if p.get("decision") == "keep_uae"),
                "delete_mislabeled_ae": sum(1 for p in plan if p.get("decision") == "delete_mislabeled_ae"),
                "skip_unknown_territory": sum(1 for p in plan if p.get("decision") == "skip_unknown_territory"),
                "skip_fetch_error": sum(1 for p in plan if p.get("decision") == "skip_fetch_error"),
            },
        }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Report actions without deleting or rebuilding reconciliation",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.35,
        metavar="SEC",
        help="Delay between BOM fetches per distinct release URL (default: 0.35)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print per-release plan rows",
    )
    args = ap.parse_args()
    result = run_cleanup(dry_run=args.dry_run, sleep_s=args.sleep, verbose=args.verbose)
    for k, v in result.items():
        if k == "plan" and not args.verbose:
            continue
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
