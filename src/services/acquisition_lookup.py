from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from rapidfuzz import fuzz
from sqlalchemy.orm import Session

from src.models import Film, RatingsMetric
from src.repositories.film_repository import FilmRepository
from src.repositories.report_repository import ReportRepository
from src.services.ingestion_service import ingest_source
from src.services.matching import TitleMatcher
from src.services.report_builder import build_film_report
from src.services.text_utils import normalize_title_cross_language
from src.services.acquisition_lookup_job import resume_acquisition_lookup_job, start_acquisition_lookup_job
from src.sources.common import ExtractedRecord
from src.sources.boxofficemojo.ingest import run_bom_weekly
from src.sources.elcinema.client import ElCinemaClient
from src.sources.elcinema.parser import parse_current_chart, parse_title_boxoffice
from src.sources.filmyard.ingest import run_filmyard_daily
from src.sources.imdb.ingest import run_imdb_daily


YEAR_BONUS = 0.06
YEAR_PENALTY = 0.05


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _title_match_score(
    *,
    query: str,
    candidate_title: str,
    release_year_hint: int | None,
    candidate_year_hint: int | None,
) -> float:
    q_cross = normalize_title_cross_language(query)
    c_cross = normalize_title_cross_language(candidate_title or "")
    score = fuzz.ratio(q_cross, c_cross) / 100.0
    if release_year_hint and candidate_year_hint:
        score += YEAR_BONUS if release_year_hint == candidate_year_hint else -YEAR_PENALTY
    return _clamp01(score)


def _coverage_from_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_source: dict[str, dict[str, Any]] = {}
    for m in matches:
        src = m.get("source_name") or "-"
        score = float(m.get("match_score") or 0.0)
        entry = by_source.setdefault(
            src,
            {
                "source_name": src,
                "matched_title_rows": 0,
                "best_match_score": 0.0,
            },
        )
        entry["matched_title_rows"] += 1
        entry["best_match_score"] = max(entry["best_match_score"], score)
    return sorted(by_source.values(), key=lambda x: (x["best_match_score"], x["matched_title_rows"]), reverse=True)


def _as_elcinema_score_records(
    *,
    query: str,
    release_year_hint: int | None,
    records: list[ExtractedRecord],
) -> list[tuple[float, ExtractedRecord]]:
    out: list[tuple[float, ExtractedRecord]] = []
    for r in records:
        cand_title = r.film_title_ar_raw or r.film_title_raw
        score = _title_match_score(
            query=query,
            candidate_title=cand_title,
            release_year_hint=release_year_hint,
            candidate_year_hint=r.release_year_hint,
        )
        out.append((score, r))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def _copy_with_confidence(record: ExtractedRecord, *, parser_confidence: float | None = None) -> ExtractedRecord:
    # ExtractedRecord is not frozen; we keep copying to avoid mutating shared objects.
    data = dict(record.__dict__)
    if parser_confidence is not None:
        data["parser_confidence"] = float(parser_confidence)
    return ExtractedRecord(**data)


@dataclass
class AcquisitionLookupResult:
    resolved_film: Film | None
    report: Any | None
    coverage_before: list[dict[str, Any]]
    coverage_after: list[dict[str, Any]]
    actions_taken: list[dict[str, Any]]


def perform_acquisition_lookup(
    session: Session,
    *,
    query: str,
    release_year_hint: int | None = None,
    imdb_title_id: str | None = None,
    elcinema_work_id_hint: str | None = None,
) -> AcquisitionLookupResult:
    if not query or not query.strip():
        raise ValueError("query is required")

    view = start_acquisition_lookup_job(
        session,
        query=query,
        release_year_hint=release_year_hint,
        imdb_title_id=imdb_title_id,
        elcinema_work_id_hint=elcinema_work_id_hint,
    )

    def _coverage_to_list(coverage: Any) -> list[dict[str, Any]]:
        if isinstance(coverage, dict):
            by_source = coverage.get("by_source") or {}
            if isinstance(by_source, dict):
                return list(by_source.values())
        return []

    coverage_before = _coverage_to_list(view.coverage)
    actions_taken: list[dict[str, Any]] = []

    # Synchronous "run to completion" wrapper for older code/tests.
    # Guard against session state corruption after internal rollbacks.
    max_iterations = 20
    for _ in range(max_iterations):
        try:
            is_active = view.job.is_active
            status = view.job.status
        except Exception:  # noqa: BLE001
            break
        if not is_active or status == "completed":
            break
        try:
            view = resume_acquisition_lookup_job(session, view.job.id)
        except Exception:  # noqa: BLE001
            break

    try:
        if view.job.notes:
            actions_taken.append({"type": "job_notes", "notes": view.job.notes})
    except Exception:  # noqa: BLE001
        pass
    try:
        ctx = view.job.context_json or {}
        dbg = (ctx.get("debug") or {})
        stages = (dbg.get("stages") or {})
        actions_taken.append(
            {
                "type": "stage_ingestion_debug",
                "stages": stages,
                "job_id": str(view.job.id),
            }
        )
    except Exception:  # noqa: BLE001
        pass
    for w in view.warnings or []:
        entry = dict(w)
        entry["type"] = "warning"  # keep stable container type; specific type stored under warning_type
        if "type" in w:
            entry["warning_type"] = w["type"]
        actions_taken.append(entry)

    coverage_after = _coverage_to_list(view.coverage)

    return AcquisitionLookupResult(
        resolved_film=view.resolved_film,
        report=view.report,
        coverage_before=coverage_before,
        coverage_after=coverage_after,
        actions_taken=actions_taken,
    )

