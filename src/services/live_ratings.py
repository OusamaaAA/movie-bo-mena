from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import Film, NormalizedEvidence, RawEvidence
from src.services.ratings_service import (
    fetch_elcinema_rating,
    fetch_imdb_rating,
    fetch_letterboxd_rating,
    save_ratings_to_db,
)


def detect_stored_ids(session: Session, film_id: str) -> tuple[str | None, str | None]:
    """Return (imdb_title_id, elcinema_work_id) from existing evidence."""
    imdb_id: str | None = None
    elc_id: str | None = None
    raw_rows = session.execute(
        select(RawEvidence)
        .join(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
        .where(NormalizedEvidence.film_id == film_id)
    ).scalars().all()
    seen_elc: set[str] = set()
    for raw in raw_rows:
        if imdb_id is None and raw.source_name == "IMDb":
            se = (raw.source_entity_id or "").strip()
            if re.match(r"tt\d+", se):
                imdb_id = se
        if elc_id is None:
            for blob in (raw.source_url or "", raw.source_entity_id or ""):
                m = re.search(r"elcinema\.com/(?:en/)?work/(\d+)", blob, re.I)
                if m and m.group(1) not in seen_elc:
                    elc_id = m.group(1)
                    seen_elc.add(elc_id)
                    break
            if elc_id is None and raw.source_name == "elCinema":
                se = (raw.source_entity_id or "").strip()
                head = se.split("_", 1)[0] if "_" in se else se
                if re.fullmatch(r"\d{4,12}", head) and head not in seen_elc:
                    elc_id = head
                    seen_elc.add(head)
    return imdb_id, elc_id


def refresh_live_ratings_for_film(
    session: Session,
    *,
    film_id: str,
    film_title: str,
    film_year: int | None,
) -> dict:
    """Fetch IMDb + elCinema + Letterboxd ratings and save to ratings_metrics."""
    detected_imdb, detected_elc = detect_stored_ids(session, film_id)
    ratings_payload: list[dict] = []
    status: list[str] = []

    r_imdb, msg_imdb = fetch_imdb_rating(film_title, film_year, imdb_id=detected_imdb)
    if r_imdb and r_imdb.get("rating") is not None:
        ratings_payload.append(r_imdb)
        status.append("IMDb:ok")
    else:
        status.append(f"IMDb:miss ({msg_imdb})")

    r_elc, msg_elc = fetch_elcinema_rating(film_title, film_year, work_id=detected_elc)
    if r_elc and r_elc.get("rating") is not None:
        ratings_payload.append(r_elc)
        status.append("elCinema:ok")
    else:
        status.append(f"elCinema:miss ({msg_elc})")

    r_lb, msg_lb = fetch_letterboxd_rating(film_title, film_year, slug=None)
    if r_lb and r_lb.get("rating") is not None:
        ratings_payload.append(r_lb)
        status.append("Letterboxd:ok")
    else:
        status.append(f"Letterboxd:miss ({msg_lb})")

    saved = save_ratings_to_db(session, film_id, ratings_payload) if ratings_payload else 0
    return {"saved": saved, "status": status}


def refresh_live_ratings_for_all_films(
    session: Session,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    """Fetch IMDb, elCinema, and Letterboxd for every film; save to ``ratings_metrics``.

    Uses the same logic as :func:`refresh_live_ratings_for_film` (auto-detected IDs from DB).
    Per-film failures are caught so the rest of the batch continues.
    """
    films = session.execute(select(Film).order_by(Film.canonical_title)).scalars().all()
    total = len(films)
    rows: list[dict[str, Any]] = []
    total_saved = 0
    for idx, f in enumerate(films):
        title = f.canonical_title or ""
        if on_progress is not None:
            on_progress(idx, total, title)
        try:
            r = refresh_live_ratings_for_film(
                session,
                film_id=f.id,
                film_title=title,
                film_year=f.release_year,
            )
            total_saved += r["saved"]
            rows.append(
                {
                    "Film": title,
                    "Saved": r["saved"],
                    "Detail": "; ".join(r["status"]),
                }
            )
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "Film": title,
                    "Saved": 0,
                    "Detail": f"error: {exc!s:.300}",
                }
            )
    return {
        "films_processed": total,
        "total_ratings_saved": total_saved,
        "rows": rows,
    }
