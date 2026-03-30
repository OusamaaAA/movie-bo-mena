from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import NormalizedEvidence, RawEvidence, ReviewQueue
from src.services.ingestion_service import rebuild_reconciled_for_film
from src.services.matching import TitleMatcher


def _get_open_review_item(session: Session, review_item_id: str) -> ReviewQueue:
    item = session.execute(select(ReviewQueue).where(ReviewQueue.id == review_item_id)).scalar_one_or_none()
    if not item:
        raise ValueError(f"Review item not found: {review_item_id}")
    if item.status != "open":
        raise ValueError(f"Review item is not open: {review_item_id} (status={item.status})")
    return item


def approve_match_to_existing_film(session: Session, review_item_id: str, *, analyst_notes: str | None = None) -> None:
    item = _get_open_review_item(session, review_item_id)
    if not item.raw_evidence_id:
        raise ValueError("Review item has no linked raw_evidence_id; cannot approve evidence.")
    if not item.candidate_film_id:
        raise ValueError("Review item candidate_film_id is empty; use create_new_film instead.")

    raw = session.get(RawEvidence, item.raw_evidence_id)
    if not raw:
        raise ValueError("Raw evidence missing for review item.")

    film_id = item.candidate_film_id
    matcher = TitleMatcher(session)
    matcher.attach_observed_titles(
        film_id=film_id,
        raw_title=raw.film_title_raw,
        raw_title_ar=raw.film_title_ar_raw,
        source_entity_id=raw.source_entity_id,
        confidence=float(item.candidate_score or 1.0),
    )

    normalized = session.execute(select(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == raw.id)).scalar_one_or_none()
    if normalized:
        normalized.film_id = film_id
        if item.candidate_score is not None:
            normalized.match_confidence = item.candidate_score

    item.status = "resolved"
    item.analyst_notes = analyst_notes
    item.resolved_at = datetime.utcnow()

    session.flush()
    rebuild_reconciled_for_film(session, film_id)


def create_new_film_from_review_item(session: Session, review_item_id: str, *, analyst_notes: str | None = None) -> str:
    item = _get_open_review_item(session, review_item_id)
    if not item.raw_evidence_id:
        raise ValueError("Review item has no linked raw_evidence_id; cannot create film.")

    raw = session.get(RawEvidence, item.raw_evidence_id)
    if not raw:
        raise ValueError("Raw evidence missing for review item.")

    matcher = TitleMatcher(session)
    film = matcher.ensure_film(raw.film_title_raw, raw.release_year_hint)
    matcher.attach_observed_titles(
        film_id=film.id,
        raw_title=raw.film_title_raw,
        raw_title_ar=raw.film_title_ar_raw,
        source_entity_id=raw.source_entity_id,
        confidence=1.0,
    )

    normalized = session.execute(select(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == raw.id)).scalar_one_or_none()
    if normalized:
        normalized.film_id = film.id
        normalized.match_confidence = item.candidate_score

    item.candidate_film_id = film.id
    item.status = "resolved"
    item.analyst_notes = analyst_notes
    item.resolved_at = datetime.utcnow()

    session.flush()
    rebuild_reconciled_for_film(session, film.id)
    return film.id


def ignore_review_item(session: Session, review_item_id: str, *, analyst_notes: str | None = None) -> None:
    item = _get_open_review_item(session, review_item_id)
    item.status = "dismissed"
    item.analyst_notes = analyst_notes
    item.resolved_at = datetime.utcnow()
    session.flush()

