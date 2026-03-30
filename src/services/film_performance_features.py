"""Intelligence layer: `film_performance_features` is computed in PostgreSQL (migrations/008_intelligence_engine_fpf.sql)."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def compute_film_performance_features(session: Session, film_id: str) -> None:
    """Call DB function ``compute_film_performance_features(uuid)`` — reads reconciled_evidence + ratings_metrics, upserts one row."""
    session.execute(
        text("SELECT compute_film_performance_features(CAST(:fid AS uuid))"),
        {"fid": film_id},
    )


def compute_all_film_performance_features(session: Session) -> None:
    """Call ``compute_all_film_performance_features()`` — recomputes features for every film."""
    session.execute(text("SELECT compute_all_film_performance_features()"))


def refresh_film_performance_features(session: Session, film_id: str) -> None:
    compute_film_performance_features(session, film_id)
    session.flush()


def refresh_film_performance_features_safe(session: Session, film_id: str) -> None:
    try:
        refresh_film_performance_features(session, film_id)
    except Exception:
        # Important: clear failed transaction state so subsequent report queries still work.
        session.rollback()
        logger.exception("film_performance_features refresh failed for film_id=%s", film_id)
