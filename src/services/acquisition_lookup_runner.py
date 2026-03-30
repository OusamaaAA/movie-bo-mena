from __future__ import annotations

from src.db import session_scope
from src.services.acquisition_lookup_job import resume_acquisition_lookup_job, start_acquisition_lookup_job


def run_acquisition_lookup(query_text: str, release_year_hint: int | None = None, imdb_title_id: str | None = None) -> dict:
    query = (query_text or "").strip()
    if not query:
        raise ValueError("query_text is required")

    try:
        with session_scope() as session:
            view = start_acquisition_lookup_job(
                session,
                query=query,
                release_year_hint=release_year_hint,
                imdb_title_id=imdb_title_id,
            )
            while view.job.is_active and view.job.status != "completed":
                view = resume_acquisition_lookup_job(session, view.job.id)
            all_matches = view.job.fast_matches_json or []
            strong_matches = [m for m in all_matches if float(m.get("match_score") or 0.0) >= 0.88][:20]
            review_matches = [m for m in all_matches if 0.70 <= float(m.get("match_score") or 0.0) < 0.88][:20]
            coverage = view.coverage.get("by_source") if isinstance(view.coverage, dict) else {}
            return {
                "query": query,
                "status": view.job.status,
                "resolved_film_id": view.job.resolved_film_id,
                "resolved_film_title": view.resolved_film.canonical_title if view.resolved_film else None,
                "created_new_film": bool(view.job.notes and "auto-created" in view.job.notes.lower()),
                "warnings": view.warnings or [],
                "strong_matches": strong_matches,
                "review_matches": review_matches,
                "coverage_by_source": list((coverage or {}).values()),
                "acquisition_summary": {
                    "job_id": view.job.id,
                    "stage": view.job.stage,
                    "notes": view.job.notes or "",
                },
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "query": query,
            "status": "failed",
            "resolved_film_id": None,
            "resolved_film_title": None,
            "created_new_film": False,
            "warnings": [{"type": "lookup_failed_exception", "message": str(exc)}],
            "strong_matches": [],
            "review_matches": [],
            "coverage_by_source": [],
            "review_queue_items_created": 0,
        }

