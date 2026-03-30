"""Persisted admissions estimates (gross ÷ ticket price) for evidence rows.

Kept separate from `ticket_pricing` so ingest/reconciliation only depend on
`estimate_admissions`, which has always lived in ticket_pricing.
"""
from __future__ import annotations

from decimal import Decimal

from src.services.ticket_pricing import estimate_admissions


def _gross_for_admissions_estimate(
    record_semantics: str,
    period_gross_local: Decimal | float | None,
    cumulative_gross_local: Decimal | float | None,
) -> float | None:
    """Align with UI tables: cumulative semantics use cumulative gross; otherwise period gross."""
    if record_semantics == "title_cumulative_total":
        g = cumulative_gross_local
    else:
        g = period_gross_local
    if g is None:
        return None
    v = float(g)
    return v if v > 0 else None


def _ticket_price_override(
    market_code: str | None,
    ticket_price_by_market_code: dict[str, Decimal | float] | None,
) -> float | None:
    if not ticket_price_by_market_code or not (market_code or "").strip():
        return None
    key = market_code.strip().upper()
    for mk, mv in ticket_price_by_market_code.items():
        if str(mk).upper() == key and mv is not None:
            return float(mv)
    return None


def admissions_estimated_for_evidence(
    *,
    admissions_actual: Decimal | float | None,
    record_semantics: str,
    period_gross_local: Decimal | float | None,
    cumulative_gross_local: Decimal | float | None,
    country_code: str | None,
    currency: str | None = None,
    ticket_price_by_market_code: dict[str, Decimal | float] | None = None,
) -> Decimal | None:
    """Persisted estimate when actual admissions are missing (same basis as Streamlit estimates)."""
    if admissions_actual is not None:
        return None
    gross = _gross_for_admissions_estimate(
        record_semantics, period_gross_local, cumulative_gross_local
    )
    if gross is None:
        return None
    code = (country_code or "").strip()
    if not code:
        return None
    override = _ticket_price_override(code, ticket_price_by_market_code)
    est = estimate_admissions(gross, code, override, gross_currency=currency)
    if est is None:
        return None
    return Decimal(str(est))
