import re
from datetime import date, datetime, timedelta, timezone

FRESHNESS_DAYS = {
    "Filmyard": 1,
    "elCinema": 7,
    "Box Office Mojo": 7,
    "IMDb": 2,
}


def compute_freshness_status(source_name: str, fetched_at: datetime | None = None) -> str:
    if not fetched_at:
        return "unknown"
    now = datetime.now(timezone.utc)
    fetched = fetched_at if fetched_at.tzinfo else fetched_at.replace(tzinfo=timezone.utc)
    age_days = (now - fetched).days
    threshold = FRESHNESS_DAYS.get(source_name, 7)
    return "fresh" if age_days <= threshold else "stale"


def derive_period_key(
    record_granularity: str,
    period_start_date: date | None,
    period_end_date: date | None,
    period_label_raw: str | None,
    release_year_hint: int | None,
) -> str:
    if period_label_raw and period_label_raw.strip():
        return period_label_raw.strip()
    g = record_granularity.lower().strip()
    if g in {"week", "weekly"} and period_start_date:
        iso = period_start_date.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    if g == "weekend" and period_start_date and period_end_date:
        return f"{period_start_date.isoformat()}..{period_end_date.isoformat()}"
    if g in {"day", "daily"} and period_start_date:
        return period_start_date.isoformat()
    if g in {"year", "yearly"} and release_year_hint:
        return str(release_year_hint)
    if g == "lifetime":
        return "lifetime"
    return ""


def period_key_to_start_date(pk: str) -> date | None:
    """Return the start date of any period_key format, or None if unparseable."""
    if not pk or pk in ("lifetime",):
        return None
    # ISO week with hyphen: 2026-W13
    m = re.match(r"(\d{4})-W(\d+)$", pk)
    if m:
        try:
            return date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    # ISO week without hyphen: 2026W13 (BOM format)
    m = re.match(r"(\d{4})W(\d+)$", pk)
    if m:
        try:
            return date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    # Date or date range: 2026-03-26 or 2026-03-26..2026-03-28
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", pk)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    # elCinema native chart week (EG/SA box-office calendar, not ISO): 2026-EW13
    m = re.match(r"(\d{4})-EW(\d+)$", pk)
    if m:
        y, ew = int(m.group(1)), int(m.group(2))
        return date(y, 1, 1) + timedelta(days=(ew - 1) * 7)
    return None


def period_key_to_iso_week(pk: str) -> str | None:
    """Convert any period_key to ISO week string (YYYY-WNN), or None if unparseable."""
    d = period_key_to_start_date(pk)
    if d is None:
        return None
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def period_key_sort_ordinal(pk: str) -> int:
    """Return a sortable integer (date ordinal) for any period_key. Lifetime sorts last."""
    d = period_key_to_start_date(pk)
    return d.toordinal() if d else date.max.toordinal()


def evidence_quality_bucket(record_scope: str, record_semantics: str, source_confidence: float | int | None) -> str:
    confidence = float(source_confidence or 0)
    if record_scope == "title" and record_semantics == "title_period_gross" and confidence >= 0.5:
        return "usable"
    if record_scope == "title" and record_semantics == "title_cumulative_total":
        return "directional"
    if record_semantics in {"market_chart_topline", "chart_signal"}:
        return "signal_only"
    return "weak"

