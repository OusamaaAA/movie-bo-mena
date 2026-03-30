from dataclasses import asdict, dataclass

from src.models import Film
from src.repositories.report_repository import ReportRepository
from src.services.semantics import evidence_quality_bucket
from src.services.scoring import ScoreBreakdown, calculate_marketing_score


@dataclass
class FilmReport:
    film: dict
    source_coverage: list[dict]
    filmyard_daily: list[dict]
    elcinema_weekly: list[dict]
    bom_signals: list[dict]
    raw_sections: dict
    market_signals: dict
    reconciled: list[dict]
    ratings: list[dict]
    marketing_inputs: list[dict]
    outcome_targets: list[dict]
    score: dict
    analyst_explanation: str
    release_intelligence: dict | None = None  # additive — populated when data is available


def build_film_report(repo: ReportRepository, film: Film) -> FilmReport:
    def _freshness_from_notes(notes: str | None) -> str | None:
        if not notes:
            return None
        if "freshness=" not in notes:
            return None
        # notes is in the format "... | freshness=<value>"
        try:
            return notes.split("freshness=", 1)[1].strip().strip("|").split(" ")[0].strip("|")
        except Exception:  # noqa: BLE001
            return None

    coverage = repo.source_coverage(film.id)
    raw_sections: dict[str, list[dict]] = {}
    for source_name, _ in coverage:
        rows = repo.raw_by_source(film.id, source_name)
        raw_sections[source_name] = [
            {
                "period_key": r.period_key,
                "rank": r.rank,
                "period_gross_local": float(r.period_gross_local or 0),
                "cumulative_gross_local": float(r.cumulative_gross_local) if r.cumulative_gross_local is not None else None,
                "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                "freshness": _freshness_from_notes(r.notes),
                "currency": r.currency,
                "record_granularity": r.record_granularity,
                "record_semantics": r.record_semantics,
                "source_url": r.source_url,
            }
            for r in rows
            if r.record_scope == "title"
        ]
    signal_rows = repo.market_signals(film.id)
    title_hints = [film.canonical_title]
    if film.canonical_title_ar:
        title_hints.append(film.canonical_title_ar)
    title_hints.extend([a.alias_text for a in film.aliases])
    for row in repo.market_signals_by_title_hints(title_hints):
        if row.id not in {x.id for x in signal_rows}:
            signal_rows.append(row)
    market_signals = {}
    for r in signal_rows:
        market_signals.setdefault(r.source_name, []).append(
            {
                "period_key": r.period_key,
                "record_scope": r.record_scope,
                "record_semantics": r.record_semantics,
                "rank": r.rank,
                "period_gross_local": float(r.period_gross_local or 0),
                "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                "freshness": _freshness_from_notes(r.notes),
                "currency": r.currency,
                "source_url": r.source_url,
                "title_hint": r.film_title_raw,
            }
        )
    filmyard_daily = raw_sections.get("Filmyard", [])
    elcinema_weekly = raw_sections.get("elCinema", [])
    # The legacy report emphasized weekend performance; in our schema BOM weekend
    # evidence is stored as title-scoped rows, so show it from raw_sections.
    bom_signals = raw_sections.get("Box Office Mojo", [])

    reconciled = repo.reconciled(film.id)
    ratings = repo.latest_ratings(film.id)
    marketing = repo.marketing_inputs(film.id)
    targets = repo.outcome_targets(film.id)
    score: ScoreBreakdown = calculate_marketing_score(reconciled, ratings, marketing, targets)
    _seen_reconciled: set[tuple] = set()
    reconciled_rows = []
    for r in reconciled:
        _key = (r.winning_source_name, r.country_code, r.period_key, r.record_granularity, r.record_semantics)
        if _key in _seen_reconciled:
            continue
        _seen_reconciled.add(_key)
        reconciled_rows.append({
            "source": r.winning_source_name,
            "country_code": r.country_code,
            "period_key": r.period_key,
            "period_start_date": r.period_start_date.isoformat() if r.period_start_date else None,
            "period_end_date": r.period_end_date.isoformat() if r.period_end_date else None,
            "period_gross_local": float(r.period_gross_local or 0),
            "cumulative_gross_local": float(r.cumulative_gross_local)
            if r.cumulative_gross_local is not None
            else None,
            "currency": r.currency,
            "granularity": r.record_granularity,
            "semantics": r.record_semantics,
            "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
            "admissions_estimated": float(r.admissions_estimated) if r.admissions_estimated is not None else None,
            "quality_bucket": evidence_quality_bucket(
                r.record_scope,
                r.record_semantics,
                1.0,
            ),
        })
    ratings_dicts = [
        {
            "source_name": x.source_name,
            "rating_value": float(x.rating_value or 0),
            "vote_count": x.vote_count,
            "popularity_rank": x.popularity_rank,
            "metric_date": str(x.metric_date),
        }
        for x in ratings
    ]

    # Release intelligence — additive read-only layer (never crashes the report)
    release_intel: dict | None = None
    try:
        from src.services.release_intelligence import compute_release_intelligence
        release_intel = compute_release_intelligence(
            repo.session,
            film.id,
            reconciled_rows=reconciled_rows,
            ratings=ratings_dicts,
            film_title=film.canonical_title or "",
        )
    except Exception:  # noqa: BLE001
        release_intel = None

    try:
        from src.services.film_performance_features import refresh_film_performance_features_safe

        refresh_film_performance_features_safe(repo.session, film.id)
    except Exception:  # noqa: BLE001
        pass

    return FilmReport(
        film={
            "id": film.id,
            "canonical_title": film.canonical_title,
            "canonical_title_ar": film.canonical_title_ar,
            "release_year": film.release_year,
        },
        source_coverage=[{"source_name": s, "rows": c} for s, c in coverage],
        filmyard_daily=filmyard_daily,
        elcinema_weekly=elcinema_weekly,
        bom_signals=bom_signals,
        raw_sections=raw_sections,
        market_signals=market_signals,
        reconciled=reconciled_rows,
        ratings=ratings_dicts,
        marketing_inputs=[
            {
                "market_code": m.market_code,
                "spend_local": float(m.spend_local or 0),
                "spend_currency": m.spend_currency,
                "campaign_start_date": str(m.campaign_start_date) if m.campaign_start_date else None,
                "campaign_end_date": str(m.campaign_end_date) if m.campaign_end_date else None,
                "notes": m.notes,
            }
            for m in marketing
        ],
        outcome_targets=[
            {
                "market_code": t.market_code,
                "target_label": t.target_label,
                "target_value": float(t.target_value or 0),
                "target_unit": t.target_unit,
                "period_start_date": str(t.period_start_date) if t.period_start_date else None,
                "period_end_date": str(t.period_end_date) if t.period_end_date else None,
            }
            for t in targets
        ],
        score=asdict(score),
        analyst_explanation=score.explanation,
        release_intelligence=release_intel,
    )

