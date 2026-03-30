"""Repair Box Office Mojo period dates that are off by year.

elCinema-style issues are handled elsewhere; this targets BOM where:
- Date labels omit the year and the parser used the *index fetch year* or ``utcnow`` instead of
  the year embedded in ``weekend_code`` (``2025W12``).
- Rows can look "plausible" (e.g. Feb 2026) while the authoritative weekend code is 2025.

Call :func:`repair_all_bom_period_issues` after BOM ingestion so operators do not rely on
manual Data Admin clicks.
"""

from __future__ import annotations

import re
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import NormalizedEvidence, RawEvidence


def _shift_date_years(d: date, delta_years: int) -> date:
    return date(d.year + delta_years, d.month, d.day)


def _shift_period_key_range_years(pk: str | None, delta_years: int) -> str | None:
    """Shift ``YYYY-MM-DD..YYYY-MM-DD`` keys by delta years on both ends."""
    if not pk or ".." not in pk:
        return pk
    parts = pk.strip().split("..", 1)
    if len(parts) != 2:
        return pk
    try:
        d1 = date.fromisoformat(parts[0])
        d2 = date.fromisoformat(parts[1])
    except ValueError:
        return pk
    return f"{_shift_date_years(d1, delta_years).isoformat()}..{_shift_date_years(d2, delta_years).isoformat()}"


def repair_bom_period_mismatch_with_weekend_code(session: Session) -> dict[str, int]:
    """Align ``period_*`` with ``weekend_code`` year from ``raw_payload_json`` when they disagree."""
    fixed_raw = 0
    affected: set[str] = set()

    raws = list(
        session.execute(
            select(RawEvidence).where(
                RawEvidence.source_name == "Box Office Mojo",
                RawEvidence.period_start_date.isnot(None),
            )
        )
        .scalars()
        .all()
    )
    for raw in raws:
        wc = (raw.raw_payload_json or {}).get("weekend_code")
        if not wc or not isinstance(wc, str):
            continue
        m = re.match(r"(20\d{2})W", wc, flags=re.I)
        if not m:
            continue
        code_year = int(m.group(1))
        ps = raw.period_start_date
        if not ps or ps.year == code_year:
            continue
        delta = code_year - ps.year
        raw.period_start_date = _shift_date_years(ps, delta)
        if raw.period_end_date:
            raw.period_end_date = _shift_date_years(raw.period_end_date, delta)
        pk = raw.period_key or ""
        if pk and re.match(r"^\d{4}-\d{2}-\d{2}\.\.", pk):
            raw.period_key = _shift_period_key_range_years(pk, delta) or pk
        fixed_raw += 1

        norm = session.execute(
            select(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == raw.id)
        ).scalars().first()
        if norm:
            if norm.period_start_date:
                norm.period_start_date = _shift_date_years(norm.period_start_date, delta)
            if norm.period_end_date:
                norm.period_end_date = _shift_date_years(norm.period_end_date, delta)
            npk = norm.period_key or ""
            if npk and re.match(r"^\d{4}-\d{2}-\d{2}\.\.", npk):
                norm.period_key = _shift_period_key_range_years(npk, delta) or npk
            if norm.film_id:
                affected.add(norm.film_id)

    from src.services.ingestion_service import rebuild_reconciled_for_film  # noqa: PLC0415

    recon = 0
    for film_id in affected:
        recon += rebuild_reconciled_for_film(session, film_id)

    return {"fixed_raw_mismatch": fixed_raw, "reconciled_after_mismatch": recon}


def repair_bom_future_dated_records(session: Session) -> dict[str, int]:
    """Shift BOM rows whose period start is still in the future (typically +1 calendar year bug)."""
    today = date.today()
    bom_future_ids = session.execute(
        select(RawEvidence.id, RawEvidence.period_key, RawEvidence.period_start_date).where(
            RawEvidence.source_name == "Box Office Mojo",
            RawEvidence.period_start_date > today,
        )
    ).all()

    fixed = 0
    affected_film_ids: set[str] = set()

    for row_id, pk, pstart in bom_future_ids:
        new_start = date(pstart.year - 1, pstart.month, pstart.day)
        new_pk = re.sub(
            r"^(\d{4})-(\d{2}-\d{2}\.\.)",
            lambda m: f"{int(m.group(1)) - 1}-{m.group(2)}",
            pk or "",
        )
        new_pk_end = re.sub(
            r"\.\.((\d{4})-(\d{2}-\d{2}))$",
            lambda m: f"..{int(m.group(2)) - 1}-{m.group(3)}",
            new_pk,
        )
        corrected_pk = new_pk_end if new_pk_end else new_pk

        raw = session.get(RawEvidence, row_id)
        if raw:
            raw.period_start_date = new_start
            if raw.period_end_date and raw.period_end_date > today:
                pend = raw.period_end_date
                raw.period_end_date = date(pend.year - 1, pend.month, pend.day)
            if pk and pk.startswith(str(pstart.year)):
                raw.period_key = corrected_pk
            fixed += 1

        norm = session.execute(
            select(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == row_id)
        ).scalars().first()
        if norm:
            norm.period_start_date = new_start
            if norm.period_end_date and norm.period_end_date > today:
                pend = norm.period_end_date
                norm.period_end_date = date(pend.year - 1, pend.month, pend.day)
            if pk and pk.startswith(str(pstart.year)):
                norm.period_key = corrected_pk
            if norm.film_id:
                affected_film_ids.add(norm.film_id)

    from src.services.ingestion_service import rebuild_reconciled_for_film  # noqa: PLC0415

    recon = 0
    for film_id in affected_film_ids:
        recon += rebuild_reconciled_for_film(session, film_id)

    return {"fixed_raw_future": fixed, "reconciled_after_future": recon}


def repair_all_bom_period_issues(session: Session) -> dict[str, int]:
    """Run mismatch repair (weekend_code wins), then future-date repair."""
    a = repair_bom_period_mismatch_with_weekend_code(session)
    b = repair_bom_future_dated_records(session)
    out: dict[str, int] = {}
    out.update(a)
    out.update(b)
    return out
