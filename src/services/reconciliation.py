from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from src.models import NormalizedEvidence, ReconciledEvidence
from src.services.admissions_estimation import admissions_estimated_for_evidence

SOURCE_PRECEDENCE = {
    'Filmyard': 100,
    'elCinema': 90,
    'Box Office Mojo': 70,
    'IMDb': 50,
}


@dataclass(frozen=True)
class ReconKey:
    film_id: str
    country_code: str | None
    record_scope: str
    record_granularity: str
    record_semantics: str
    evidence_type: str
    period_key: str | None


def _as_float(value: Decimal | float | None) -> float:
    return float(value or 0)


def _pick_winner(rows: list[NormalizedEvidence]) -> NormalizedEvidence:
    def sort_key(row: NormalizedEvidence) -> tuple[float, float, float]:
        # closer to the spreadsheet logic: precedence first, then confidence, then informational richness.
        richness = 0.0
        if row.period_gross_local is not None:
            richness += 1.0
        if row.cumulative_gross_local is not None:
            richness += 0.5
        return (
            float(SOURCE_PRECEDENCE.get(row.source_name, 0)),
            float(row.source_confidence or 0),
            richness,
        )

    return sorted(rows, key=sort_key, reverse=True)[0]


def reconcile_records(
    rows: list[NormalizedEvidence],
    ticket_price_by_market_code: dict[str, Decimal | float] | None = None,
) -> list[ReconciledEvidence]:
    """Keep the strict comparability rule from the .gs engine.

    - never reconcile incomparable semantics together
    - market_chart_topline is signal-only, not a performance winner
    - title_period_gross and title_cumulative_total remain distinct
    """
    buckets: dict[ReconKey, list[NormalizedEvidence]] = defaultdict(list)
    for row in rows:
        if not row.film_id:
            continue
        key = ReconKey(
            film_id=row.film_id,
            country_code=row.country_code,
            record_scope=row.record_scope,
            record_granularity=row.record_granularity,
            record_semantics=row.record_semantics,
            evidence_type=row.evidence_type,
            period_key=row.period_key,
        )
        buckets[key].append(row)

    ticket_price_by_market_code = ticket_price_by_market_code or {}

    output: list[ReconciledEvidence] = []
    for key, comparable in buckets.items():
        if not comparable:
            continue
        first = comparable[0]
        if not all(
            c.record_scope == first.record_scope
            and c.record_granularity == first.record_granularity
            and c.record_semantics == first.record_semantics
            and c.evidence_type == first.evidence_type
            for c in comparable
        ):
            continue

        # Signal-only rows should remain visible elsewhere, not become reconciled performance winners.
        if first.record_semantics == 'market_chart_topline':
            continue

        winner = _pick_winner(comparable)

        admissions_actual = winner.admissions_actual
        admissions_estimated = admissions_estimated_for_evidence(
            admissions_actual=admissions_actual,
            record_semantics=winner.record_semantics,
            period_gross_local=winner.period_gross_local,
            cumulative_gross_local=winner.cumulative_gross_local,
            country_code=winner.country_code,
            currency=winner.currency,
            ticket_price_by_market_code=ticket_price_by_market_code,
        )
        output.append(
            ReconciledEvidence(
                film_id=winner.film_id or '',
                source_fingerprint='|'.join(
                    str(x) for x in [
                        winner.film_id or '',
                        winner.country_code or '',
                        winner.record_scope,
                        winner.record_granularity,
                        winner.record_semantics,
                        winner.period_key or '',
                    ]
                ),
                country_code=winner.country_code,
                record_scope=winner.record_scope,
                record_granularity=winner.record_granularity,
                record_semantics=winner.record_semantics,
                evidence_type=winner.evidence_type,
                period_start_date=winner.period_start_date,
                period_end_date=winner.period_end_date,
                period_key=winner.period_key,
                rank=winner.rank,
                period_gross_local=winner.period_gross_local,
                cumulative_gross_local=winner.cumulative_gross_local,
                currency=winner.currency,
                admissions_actual=admissions_actual,
                admissions_estimated=admissions_estimated,
                winning_source_name=winner.source_name,
                contributing_sources=sorted({v.source_name for v in comparable}),
                explanation=(
                    'Selected from comparable records using source precedence and confidence. '
                    'Signal-only market chart rows are excluded from reconciled performance winners.'
                ),
            )
        )
    return output
