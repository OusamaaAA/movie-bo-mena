import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from src.models import Film, FilmAlias, MarketReference, NormalizedEvidence, RatingsMetric, RawEvidence, ReconciledEvidence
from src.repositories.ingest_repository import IngestRepository
from src.schema_types import RunStatus
from src.services.matching import TitleMatcher
from src.services.reconciliation import reconcile_records
from src.services.semantics import compute_freshness_status, derive_period_key
from src.services.admissions_estimation import admissions_estimated_for_evidence
from src.services.text_utils import normalize_title
from src.sources.boxofficemojo.ingest import run_bom_backfill, run_bom_weekly, run_bom_backfill_year_range
from src.sources.elcinema.ingest import run_elcinema_backfill, run_elcinema_weekly
from src.sources.filmyard.ingest import run_filmyard_backfill, run_filmyard_daily
from src.sources.imdb.ingest import run_imdb_daily
from src.sources.common import ExtractedRecord

# Deterministic idempotency keys must be valid UUID strings for Postgres `uuid` PK columns.
_RAW_EVIDENCE_STABLE_NS = uuid.UUID("a3f8c2e1-4b5d-4c9e-8f2a-1d0e9b7c6a54")


def _ticket_prices_from_session(session: Session) -> dict[str, Decimal | float]:
    return {
        str(r.market_code): r.value_num
        for r in session.execute(
            select(MarketReference).where(MarketReference.reference_type == "ticket_price")
        ).scalars().all()
        if r.value_num is not None
    }


def _to_raw(
    *,
    run_id: str,
    record: ExtractedRecord,
    fetched_at: datetime,
    ticket_price_by_market_code: dict[str, Decimal | float] | None = None,
) -> RawEvidence:
    period_key = record.period_key or derive_period_key(
        record_granularity=record.record_granularity,
        period_start_date=record.period_start_date,
        period_end_date=record.period_end_date,
        period_label_raw=record.period_label_raw,
        release_year_hint=record.release_year_hint,
    )
    # Single canonical identity for elCinema weekly rows (no parallel YYYY-W / YYYY-EW keys).
    if (record.source_name or "") == "elCinema" and record.period_start_date and record.period_end_date:
        g = (record.record_granularity or "").lower()
        if g in {"week", "weekly"}:
            period_key = (
                f"{record.period_start_date.isoformat()}..{record.period_end_date.isoformat()}"
            )
    payload = dict(record.raw_payload_json or {})
    if record.period_start_date:
        payload.setdefault("snapshot_date", record.period_start_date.isoformat())

    # Idempotency without relying on optional DB columns:
    # Use a deterministic primary key for title-scoped evidence.
    raw_id: str | None = None
    if record.record_scope == "title":
        stable_key = "|".join(
            [
                record.source_name or "",
                record.country_code or "",
                str(record.source_entity_id or ""),
                normalize_title(record.film_title_raw or ""),
                str(record.release_year_hint or ""),
                record.record_semantics or "",
                str(period_key or ""),
                record.record_granularity or "",
                record.evidence_type or "",
            ]
        )
        # Deterministic UUID (RFC 4122) — sha1[:36] hex is not valid for Postgres uuid type.
        raw_id = str(uuid.uuid5(_RAW_EVIDENCE_STABLE_NS, stable_key))

    args = dict(
        source_run_id=run_id,
        source_name=record.source_name,
        source_url=record.source_url,
        source_entity_id=record.source_entity_id,
        country_code=record.country_code,
        film_title_raw=record.film_title_raw,
        film_title_ar_raw=record.film_title_ar_raw,
        release_year_hint=record.release_year_hint,
        record_scope=record.record_scope,
        record_granularity=record.record_granularity,
        record_semantics=record.record_semantics,
        evidence_type=record.evidence_type,
        period_label_raw=record.period_label_raw,
        period_start_date=record.period_start_date,
        period_end_date=record.period_end_date,
        period_key=period_key,
        rank=record.rank,
        period_gross_local=Decimal(str(record.period_gross_local))
        if record.period_gross_local is not None
        else None,
        cumulative_gross_local=Decimal(str(record.cumulative_gross_local))
        if record.cumulative_gross_local is not None
        else None,
        currency=record.currency,
        admissions_actual=Decimal(str(record.admissions_actual))
        if record.admissions_actual is not None
        else None,
        parser_confidence=Decimal(str(record.parser_confidence)),
        source_confidence=Decimal(str(record.source_confidence)),
        notes=f"{record.notes or ''} | freshness={compute_freshness_status(record.source_name, fetched_at)}".strip(" |"),
        raw_payload_json=payload,
    )
    args["admissions_estimated"] = admissions_estimated_for_evidence(
        admissions_actual=args["admissions_actual"],
        record_semantics=record.record_semantics,
        period_gross_local=record.period_gross_local,
        cumulative_gross_local=record.cumulative_gross_local,
        country_code=record.country_code,
        currency=record.currency,
        ticket_price_by_market_code=ticket_price_by_market_code,
    )
    if raw_id:
        args["id"] = raw_id
    return RawEvidence(**args)


def _rebuild_reconciled_for_films(session: Session, film_ids: set[str]) -> int:
    total = 0
    for film_id in film_ids:
        total += rebuild_reconciled_for_film(session, film_id)
    return total


def ingest_source(session: Session, source_code: str, run_type: str, records: list) -> dict:
    repo = IngestRepository(session)
    matcher = TitleMatcher(session)
    run = repo.create_run(source_code=source_code, run_type=run_type)
    fetched_count = 0
    normalized_count = 0
    reconciled_count = 0
    try:
        fetched_at = run.started_at
        ticket_prices = _ticket_prices_from_session(session)
        raw_rows: list[RawEvidence] = []
        for r in records:
            raw_rows.append(
                _to_raw(
                    run_id=run.id,
                    record=r,
                    fetched_at=fetched_at,
                    ticket_price_by_market_code=ticket_prices,
                )
            )
        inserted_raw_rows = repo.add_raw_records(raw_rows)
        fetched_count = len(inserted_raw_rows)
        raw_rows = inserted_raw_rows
        normalized_rows: list[NormalizedEvidence] = []
        touched_film_ids: set[str] = set()
        for raw in raw_rows:
            if raw.record_scope == "title" and raw.film_title_raw:
                match = matcher.match_or_queue(
                    raw.film_title_raw,
                    raw.release_year_hint,
                    raw.id,
                    record_scope="title",
                    raw_title_ar=raw.film_title_ar_raw,
                    source_entity_id=raw.source_entity_id,
                    parser_confidence=float(raw.parser_confidence) if raw.parser_confidence is not None else None,
                    source_confidence=float(raw.source_confidence) if raw.source_confidence is not None else None,
                )
                film_id = match.film_id
                match_conf = Decimal(str(match.confidence))
                match_reason = match.reason
            elif raw.record_scope != "title":
                film_id = None
                match_conf = Decimal("0")
                match_reason = "not_applicable_non_title_scope"
            else:
                film_id = None
                match_conf = Decimal("0")
                match_reason = "no_title_text"
            if film_id:
                touched_film_ids.add(film_id)
            normalized_rows.append(
                NormalizedEvidence(
                    raw_evidence_id=raw.id,
                    film_id=film_id,
                    source_name=raw.source_name,
                    country_code=raw.country_code,
                    record_scope=raw.record_scope,
                    record_granularity=raw.record_granularity,
                    record_semantics=raw.record_semantics,
                    evidence_type=raw.evidence_type,
                    period_start_date=raw.period_start_date,
                    period_end_date=raw.period_end_date,
                    period_key=raw.period_key,
                    period_gross_local=raw.period_gross_local,
                    cumulative_gross_local=raw.cumulative_gross_local,
                    currency=raw.currency,
                    rank=raw.rank,
                    admissions_actual=raw.admissions_actual,
                    admissions_estimated=raw.admissions_estimated,
                    parser_confidence=raw.parser_confidence,
                    source_confidence=raw.source_confidence,
                    match_confidence=match_conf,
                    normalized_payload_json={"match_reason": match_reason},
                )
            )
        normalized_count = repo.add_normalized_records(normalized_rows)
        reconciled_count = _rebuild_reconciled_for_films(session, touched_film_ids)
        repo.close_run(
            run,
            status=RunStatus.SUCCESS,
            fetched=fetched_count,
            normalized=normalized_count,
            reconciled=reconciled_count,
        )
        result: dict = {
            "run_id": run.id,
            "fetched": fetched_count,
            "normalized": normalized_count,
            "reconciled": reconciled_count,
        }
        if source_code == "bom":
            from src.services.bom_period_repair import repair_all_bom_period_issues  # noqa: PLC0415

            result["bom_period_repair"] = repair_all_bom_period_issues(session)
        return result
    except Exception as exc:  # noqa: BLE001
        # PostgreSQL aborts the whole transaction on any SQL error; without rollback,
        # the session stays unusable and the next ingest_source (e.g. bom after elcinema)
        # fails with InFailedSqlTransaction on unrelated SELECTs.
        session.rollback()
        try:
            fail_run = repo.create_run(source_code=source_code, run_type=run_type)
            repo.close_run(
                fail_run,
                status=RunStatus.FAILED,
                fetched=fetched_count,
                normalized=normalized_count,
                reconciled=reconciled_count,
                error=str(exc),
            )
            session.flush()
        except Exception as log_exc:  # noqa: BLE001
            session.rollback()
            raise exc from log_exc
        return {"run_id": fail_run.id, "status": "failed", "error": str(exc)}


def _resolve_film_for_imdb_reference(session: Session, ref: MarketReference) -> Film | None:
    # Preferred path: period_key carries film_id.
    if ref.period_key:
        film = session.get(Film, ref.period_key)
        if film:
            return film
    # Fallback: source_name carries a canonical title or alias hint.
    if ref.source_name:
        norm = normalize_title(ref.source_name)
        film = session.execute(select(Film).where(Film.normalized_title == norm)).scalar_one_or_none()
        if film:
            return film
        alias = session.execute(select(FilmAlias).where(FilmAlias.normalized_alias == norm)).scalar_one_or_none()
        if alias:
            return session.get(Film, alias.film_id)
    return None


def run_daily(session: Session) -> list[dict]:
    result = []
    result.append(ingest_source(session, "filmyard", "daily", run_filmyard_daily()))
    # Ratings refresh for films with tracked IMDb title ids in market_reference.
    tracked = list(
        session.execute(
            select(MarketReference).where(MarketReference.reference_type == "imdb_title_id")
        ).scalars().all()
    )
    refreshed = 0
    skipped = 0
    for ref in tracked:
        if not ref.value_text or not ref.value_text.startswith("tt"):
            skipped += 1
            continue
        film = _resolve_film_for_imdb_reference(session, ref)
        if not film:
            skipped += 1
            continue
        metrics = run_imdb_daily(ref.value_text, fallback_title=film.canonical_title)
        for metric in metrics:
            session.add(
                RatingsMetric(
                    film_id=film.id,
                    source_name=metric.source_name,
                    rating_value=metric.rating_value,
                    vote_count=metric.vote_count,
                    popularity_rank=metric.popularity_rank,
                    metric_date=date.today(),
                    raw_payload_json=metric.payload,
                )
            )
            refreshed += 1
    result.append({"ratings_refreshed": refreshed, "ratings_skipped": skipped})
    return result


def run_weekly(session: Session) -> list[dict]:
    result = []
    result.append(ingest_source(session, "elcinema", "weekly", run_elcinema_weekly()))
    result.append(ingest_source(session, "bom", "weekly", run_bom_weekly()))
    return result


def run_backfill(session: Session, source_code: str, days: int) -> dict:
    if source_code == "filmyard":
        records = run_filmyard_backfill(days)
    elif source_code == "elcinema":
        records = run_elcinema_backfill(days)
    elif source_code == "bom":
        records = run_bom_backfill(max(1, days // 7))
    else:
        records = []
    return ingest_source(session, source_code, f"backfill_{days}d", records)


def run_bom_backfill_year_range_job(
    session: Session,
    *,
    start_year: int,
    end_year: int,
    market_codes: list[str] | None = None,
) -> dict:
    records = run_bom_backfill_year_range(start_year=start_year, end_year=end_year, market_codes=market_codes)
    run_type = f"bom_backfill_{start_year}_{end_year}"
    return ingest_source(session, "bom", run_type, records)


def refresh_title(session: Session, title: str, imdb_title_id: str | None = None) -> dict:
    if imdb_title_id:
        metrics = run_imdb_daily(imdb_title_id, fallback_title=title)
        matcher = TitleMatcher(session)
        film = matcher.ensure_film(title, release_year=None)
        for metric in metrics:
            session.add(
                RatingsMetric(
                    film_id=film.id,
                    source_name=metric.source_name,
                    rating_value=metric.rating_value,
                    vote_count=metric.vote_count,
                    popularity_rank=metric.popularity_rank,
                    metric_date=date.today(),
                    raw_payload_json=metric.payload,
                )
            )
    return {"title": title, "refreshed": True}


def rebuild_reconciled_for_film(session: Session, film_id: str) -> int:
    rows = list(
        session.execute(
            select(NormalizedEvidence).where(NormalizedEvidence.film_id == film_id)
        ).scalars().all()
    )
    ticket_prices = _ticket_prices_from_session(session)
    session.execute(delete(ReconciledEvidence).where(ReconciledEvidence.film_id == film_id))
    recon = reconcile_records(rows, ticket_price_by_market_code=ticket_prices)
    session.add_all(recon)
    return len(recon)

