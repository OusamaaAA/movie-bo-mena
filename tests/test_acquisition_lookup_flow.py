from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, Film, FilmAlias, LookupJob, ReviewQueue, Source
from src.services import acquisition_lookup_job as jobmod
from src.services.acquisition_lookup_runner import run_acquisition_lookup
from src.sources.common import ExtractedRecord


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return SessionLocal()


def _seed_sources(session: Session) -> None:
    session.add_all(
        [
            Source(source_code="elcinema", source_name="elCinema", source_family="boxoffice", schedule_type="weekly", is_active=True),
            Source(source_code="bom", source_name="Box Office Mojo", source_family="boxoffice", schedule_type="weekly", is_active=True),
            Source(source_code="filmyard", source_name="Filmyard", source_family="boxoffice", schedule_type="daily", is_active=True),
        ]
    )
    session.flush()


def _title_rec(title: str, source: str = "elCinema", entity: str = "x1", year: int = 2024) -> ExtractedRecord:
    return ExtractedRecord(
        source_name=source,
        source_url="https://example.test",
        source_entity_id=entity,
        country_code="EG",
        film_title_raw=title,
        film_title_ar_raw=None,
        release_year_hint=year,
        record_scope="title",
        record_granularity="week",
        record_semantics="title_period_gross",
        evidence_type="title_performance",
        period_label_raw="2024-W01",
        period_start_date=date(2024, 1, 1),
        period_end_date=date(2024, 1, 7),
        period_key="2024-W01",
        rank=1,
        period_gross_local=100.0,
        cumulative_gross_local=200.0,
        currency="EGP",
        admissions_actual=None,
        parser_confidence=0.9,
        source_confidence=0.9,
        notes="test",
        raw_payload_json={},
    )


def test_exact_alias_hit_resolves_without_live_lookup(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    film = Film(canonical_title="Alpha", normalized_title="alpha", release_year=2024)
    s.add(film)
    s.flush()
    s.add(FilmAlias(film_id=film.id, alias_text="Alpha", normalized_alias="alpha", alias_type="seed", alias_language="en", source="seed"))
    s.flush()

    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [{"film_id": film.id, "match_score": 0.99, "title": "Alpha", "source_name": "Filmyard"}])
    view = jobmod.start_acquisition_lookup_job(s, query="Alpha", release_year_hint=2024, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert view.job.resolved_film_id == film.id


def test_weak_stored_evidence_triggers_live_discovery(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [{"film_id": None, "match_score": 0.51, "title": "Beta", "source_name": "Filmyard"}])
    monkeypatch.setattr(
        jobmod,
        "discover_elcinema_candidates",
        lambda *_args, **_kwargs: ([{"entity_id": "123", "url": "https://elcinema.com/en/work/123/"}], {}),
    )
    monkeypatch.setattr(jobmod, "discover_bom_candidates_bundle", lambda *_args, **_kwargs: ([], [], {}))
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_title_boxoffice", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(jobmod, "parse_title_boxoffice", lambda *_args, **_kwargs: [])
    view = jobmod.start_acquisition_lookup_job(s, query="Beta", release_year_hint=2024, imdb_title_id=None)
    assert (view.job.context_json or {}).get("elcinema_candidates")


def test_ambiguous_case_goes_to_review_queue(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    f1 = Film(canonical_title="Gamma 1", normalized_title="gamma 1", release_year=2024)
    f2 = Film(canonical_title="Gamma 2", normalized_title="gamma 2", release_year=2024)
    s.add_all([f1, f2])
    s.flush()
    matches = [
        {"film_id": f1.id, "match_score": 0.85, "title": "Gamma", "source_name": "elCinema", "raw_evidence_id": "rx1"},
        {"film_id": f2.id, "match_score": 0.84, "title": "Gamma", "source_name": "Box Office Mojo", "raw_evidence_id": "rx2"},
    ]
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: matches)
    monkeypatch.setattr(jobmod, "discover_elcinema_candidates", lambda *_args, **_kwargs: ([], {}))
    monkeypatch.setattr(jobmod, "discover_bom_candidates_bundle", lambda *_args, **_kwargs: ([], [], {}))
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_boxoffice_chart", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        jobmod,
        "_get_or_create_review_item",
        lambda session, **kwargs: session.add(
            ReviewQueue(
                raw_evidence_id=None,
                film_title_raw=kwargs.get("film_title_raw") or "Gamma",
                release_year_hint=kwargs.get("release_year_hint"),
                candidate_film_id=kwargs.get("candidate_film_id"),
                candidate_score=kwargs.get("candidate_score"),
                reason=kwargs.get("reason") or "ambiguous",
            )
        ),
    )
    view = jobmod.start_acquisition_lookup_job(s, query="Gamma", release_year_hint=2024, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert s.query(ReviewQueue).count() >= 1


def test_no_existing_film_strong_live_evidence_creates_new(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(
        jobmod,
        "_compute_fast_matches",
        lambda *_args, **_kwargs: [{"film_id": None, "match_score": 0.92, "title": "New Delta", "source_name": "elCinema"}],
    )
    monkeypatch.setattr(jobmod, "discover_elcinema_candidates", lambda *_args, **_kwargs: ([], {}))
    monkeypatch.setattr(jobmod, "discover_bom_candidates_bundle", lambda *_args, **_kwargs: ([], [], {}))
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_boxoffice_chart", lambda *_args, **_kwargs: "")
    view = jobmod.start_acquisition_lookup_job(s, query="New Delta", release_year_hint=2025, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert view.job.resolved_film_id is not None
    assert s.get(Film, view.job.resolved_film_id) is not None


def test_partial_source_failure_returns_structured_warnings(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(jobmod, "discover_elcinema_candidates", lambda *_args, **_kwargs: ([{"entity_id": "x", "url": "u"}], {}))
    monkeypatch.setattr(jobmod, "discover_bom_candidates_bundle", lambda *_args, **_kwargs: ([], [], {}))
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_title_boxoffice", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    view = jobmod.start_acquisition_lookup_job(s, query="Err", release_year_hint=2024, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert isinstance(view.warnings, list)


def test_elcinema_discovery_enrichment_path_runs(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        jobmod,
        "discover_elcinema_candidates",
        lambda *_args, **_kwargs: ([{"entity_id": "333", "url": "https://elcinema.com/en/work/333/", "resolved_score": 0.95}], {}),
    )
    monkeypatch.setattr(jobmod, "discover_bom_candidates_bundle", lambda *_args, **_kwargs: ([], [], {}))
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_title_boxoffice", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_boxoffice_chart", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(jobmod, "parse_title_boxoffice", lambda *_args, **_kwargs: [_title_rec("Echo", "elCinema", "333")])
    view = jobmod.start_acquisition_lookup_job(s, query="Echo", release_year_hint=2024, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert view.job.status == "completed"


def test_bom_title_and_release_stage_ingest(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(jobmod, "discover_elcinema_candidates", lambda *_args, **_kwargs: ([], {}))
    monkeypatch.setattr(
        jobmod,
        "discover_bom_candidates_bundle",
        lambda *_args, **_kwargs: (
            [{"url": "https://www.boxofficemojo.com/title/tt1/", "resolved_score": 0.95, "resolved_title_en": "Foxtrot"}],
            [
                {
                    "url": "https://www.boxofficemojo.com/release/rl1/weekend/",
                    "resolved_score": 0.92,
                    "title_url": "https://www.boxofficemojo.com/title/tt1/",
                },
            ],
            {},
        ),
    )
    monkeypatch.setattr(jobmod.ElCinemaClient, "fetch_boxoffice_chart", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(jobmod.BomClient, "fetch_title_page", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(jobmod.BomClient, "fetch_release_page", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(jobmod, "parse_title_page_candidate", lambda *_args, **_kwargs: {"title_en": "Foxtrot", "release_year": 2024, "intl_gross_usd": 12000.0, "release_urls": ["https://www.boxofficemojo.com/release/rl1/weekend/"]})
    monkeypatch.setattr(jobmod, "parse_release_page_evidence", lambda *_args, **_kwargs: [_title_rec("Foxtrot", "Box Office Mojo", "rl1", 2024)])
    view = jobmod.start_acquisition_lookup_job(s, query="Foxtrot", release_year_hint=2024, imdb_title_id=None)
    while view.job.is_active:
        view = jobmod.resume_acquisition_lookup_job(s, view.job.id)
    assert view.job.status == "completed"


def test_runner_result_shape_matches_staged(monkeypatch) -> None:
    # runner has its own session; patch to force deterministic failure shape
    monkeypatch.setattr(jobmod, "start_acquisition_lookup_job", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))
    result = run_acquisition_lookup("Shape Test", 2024, None)
    assert set(["query", "status", "resolved_film_id", "resolved_film_title", "created_new_film", "warnings", "strong_matches", "review_matches", "coverage_by_source"]).issubset(set(result.keys()))
