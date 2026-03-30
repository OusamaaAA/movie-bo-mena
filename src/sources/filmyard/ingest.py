from datetime import date, timedelta

from src.services.semantics import period_key_to_iso_week
from src.sources.common import ExtractedRecord
from src.sources.filmyard.client import FilmyardClient
from src.sources.filmyard.parser import parse_daily_egypt


def _with_snapshot_date(records: list[ExtractedRecord], fetch_date: date) -> list[ExtractedRecord]:
    """Attach fetch-time snapshot to payload; period fields use data date from parser when set."""
    out: list[ExtractedRecord] = []
    for r in records:
        payload = dict(r.raw_payload_json or {})
        payload["snapshot_date"] = fetch_date.isoformat()

        # Revenue rows include the true box-office day; parser sets period_start_date from that.
        anchor = r.period_start_date or fetch_date

        # Weekly records use the ISO week as their period_key so that multiple
        # daily fetches within the same week upsert the same row (dedup by period_key).
        if r.record_granularity in ("week", "weekly"):
            pk = period_key_to_iso_week(anchor.isoformat()) or anchor.isoformat()
            period_label = pk
            # Week runs Mon–Sun; use Monday as canonical start date.
            iso = anchor.isocalendar()
            week_start = date.fromisocalendar(iso.year, iso.week, 1)
            week_end = date.fromisocalendar(iso.year, iso.week, 7)
        else:
            pk = anchor.isoformat()
            period_label = anchor.isoformat()
            week_start = anchor
            week_end = anchor

        out.append(
            ExtractedRecord(
                source_name=r.source_name,
                source_url=r.source_url,
                source_entity_id=r.source_entity_id,
                country_code=r.country_code,
                film_title_raw=r.film_title_raw,
                film_title_ar_raw=r.film_title_ar_raw,
                release_year_hint=r.release_year_hint or anchor.year,
                record_scope=r.record_scope,
                record_granularity=r.record_granularity,
                record_semantics=r.record_semantics,
                evidence_type=r.evidence_type,
                period_label_raw=period_label,
                period_start_date=week_start,
                period_end_date=week_end,
                period_key=pk,
                rank=r.rank,
                period_gross_local=r.period_gross_local,
                cumulative_gross_local=r.cumulative_gross_local,
                currency=r.currency,
                admissions_actual=r.admissions_actual,
                parser_confidence=r.parser_confidence,
                source_confidence=r.source_confidence,
                notes=r.notes,
                raw_payload_json=payload,
            )
        )
    return out


def run_filmyard_daily() -> list:
    client = FilmyardClient()
    html = client.fetch_daily_egypt_page()
    parsed = parse_daily_egypt(html, f"{client.base_url}/box-office")
    return _with_snapshot_date(parsed, date.today())


def run_filmyard_backfill(days: int) -> list:
    client = FilmyardClient()
    output = []
    for offset in range(days):
        d = date.today() - timedelta(days=offset)
        try:
            html = client.fetch_archive_day(d.year, d.month, d.day)
            parsed = parse_daily_egypt(html, f"{client.base_url}/box-office")
            output.extend(_with_snapshot_date(parsed, d))
        except Exception:  # noqa: BLE001
            # Missing archive day is expected for some dates.
            continue
    return output

