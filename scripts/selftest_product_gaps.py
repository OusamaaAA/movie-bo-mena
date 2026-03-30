from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import (
    Base,
    Film,
    FilmAlias,
    MarketReference,
    NormalizedEvidence,
    RawEvidence,
    ReconciledEvidence,
    Source,
    RatingsMetric,
    ReviewQueue,
)
from src.repositories.film_repository import FilmRepository
from src.repositories.report_repository import ReportRepository
from src.services.ingestion_service import ingest_source
from src.services.matching import TitleMatcher
from src.services.acquisition_lookup_job import resume_acquisition_lookup_job, start_acquisition_lookup_job
from src.sources.common import ExtractedRecord
from src.sources.imdb.models import ImdbMetric


def make_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return SessionLocal()


def seed_sources(session: Session) -> None:
    session.add_all(
        [
            Source(
                source_code="filmyard",
                source_name="Filmyard",
                source_family="boxoffice_daily",
                schedule_type="daily",
                is_active=True,
            ),
            Source(
                source_code="elcinema",
                source_name="elCinema",
                source_family="boxoffice_weekly",
                schedule_type="weekly",
                is_active=True,
            ),
            Source(
                source_code="bom",
                source_name="Box Office Mojo",
                source_family="market_boxoffice",
                schedule_type="weekly",
                is_active=True,
            ),
        ]
    )
    session.flush()


def test_transliteration_normalized_match() -> None:
    session = make_session()
    seed_sources(session)

    film = Film(canonical_title="Bershama", normalized_title="bershama", release_year=2020)
    session.add(film)
    session.flush()
    session.add(
        FilmAlias(
            film_id=film.id,
            alias_text="برشامة",
            normalized_alias="برشامة",
            alias_language="ar",
            alias_type="seed",
            confidence=1.0,
            source="seed",
        )
    )
    session.commit()

    matcher = TitleMatcher(session)
    res = matcher.match_or_queue(
        "Bershama",
        release_year_hint=2020,
        raw_evidence_id=None,
        record_scope="title",
        parser_confidence=0.1,
        source_confidence=0.1,
    )
    assert res.film_id == film.id
    assert res.needs_review is False


def test_auto_create_new_film() -> None:
    session = make_session()
    seed_sources(session)

    matcher = TitleMatcher(session)
    res = matcher.match_or_queue(
        "New Movie Title",
        release_year_hint=2024,
        raw_evidence_id="dummy_raw_1",
        record_scope="title",
        source_entity_id="work_123",
        parser_confidence=0.88,
        source_confidence=0.80,
    )
    assert res.film_id is not None
    created = session.get(Film, res.film_id)
    assert created is not None
    assert created.canonical_title == "New Movie Title"


def test_dedupe_idempotent() -> None:
    session = make_session()
    seed_sources(session)

    record = ExtractedRecord(
        source_name="Filmyard",
        source_url="https://example.test",
        source_entity_id="work_1",
        country_code="EG",
        film_title_raw="Dedupe Film",
        film_title_ar_raw=None,
        release_year_hint=2024,
        record_scope="title",
        record_granularity="day",
        record_semantics="title_period_gross",
        evidence_type="title_performance",
        period_label_raw="2024-01-01",
        period_start_date=date(2024, 1, 1),
        period_end_date=date(2024, 1, 1),
        period_key="2024-01-01",
        rank=1,
        period_gross_local=100.0,
        cumulative_gross_local=200.0,
        currency="EGP",
        admissions_actual=10.0,
        parser_confidence=0.97,
        source_confidence=0.92,
        notes="test",
        raw_payload_json={},
    )

    ingest_source(session, "filmyard", "daily", [record])
    session.commit()
    ingest_source(session, "filmyard", "daily", [record])
    session.commit()

    normalized_count = session.query(NormalizedEvidence).count()
    assert normalized_count == 1


def test_estimated_admissions_for_gross_only() -> None:
    session = make_session()
    seed_sources(session)
    session.add(
        MarketReference(
            market_code="EG",
            reference_type="ticket_price",
            value_num=50.0,
            value_text=None,
            period_key=None,
            source_name="seed",
        )
    )
    session.commit()

    record = ExtractedRecord(
        source_name="Box Office Mojo",
        source_url="https://example.test",
        source_entity_id="/release/tt123",
        country_code="EG",
        film_title_raw="Gross Only Film",
        film_title_ar_raw=None,
        release_year_hint=2024,
        record_scope="title",
        record_granularity="weekend",
        record_semantics="title_period_gross",
        evidence_type="title_performance",
        period_label_raw="2024W01",
        period_start_date=None,
        period_end_date=None,
        period_key="2024W01",
        rank=1,
        period_gross_local=1000.0,
        cumulative_gross_local=2000.0,
        currency="USD",
        admissions_actual=None,
        parser_confidence=0.88,
        source_confidence=0.80,
        notes="test",
        raw_payload_json={},
    )

    ingest_source(session, "bom", "bom_backfill_test", [record])
    session.commit()

    recon = session.query(ReconciledEvidence).all()
    assert len(recon) == 1
    row = recon[0]
    assert row.admissions_actual is None
    assert row.admissions_estimated is not None
    assert float(row.admissions_estimated) == 20.0


def test_acquisition_lookup_evidence_matches() -> None:
    session = make_session()
    seed_sources(session)

    raw = RawEvidence(
        source_run_id=None,
        source_name="Filmyard",
        source_url="https://example.test",
        source_entity_id=None,
        country_code="EG",
        film_title_raw="برشامة",
        film_title_ar_raw=None,
        release_year_hint=2020,
        record_scope="title",
        record_granularity="day",
        record_semantics="title_period_gross",
        evidence_type="title_performance",
        period_label_raw="2024-01-01",
        period_start_date=date(2024, 1, 1),
        period_end_date=date(2024, 1, 1),
        period_key="2024-01-01",
        rank=1,
        period_gross_local=100.0,
        cumulative_gross_local=200.0,
        currency="EGP",
        admissions_actual=10.0,
        admissions_estimated=None,
        parser_confidence=0.9,
        source_confidence=0.9,
        match_confidence=None,
        notes="test | freshness=fresh",
        raw_payload_json={},
    )
    session.add(raw)
    session.commit()

    matches = ReportRepository(session).title_evidence_matches(
        "Bershama", limit=3, sample_size=50, release_year_hint=2020
    )
    assert matches
    assert matches[0]["title"] == "برشامة"
    assert matches[0]["match_score"] >= 0.9


def test_film_search_cross_language_reranking() -> None:
    session = make_session()
    seed_sources(session)

    film = Film(canonical_title="Bershama", normalized_title="bershama", release_year=2020)
    session.add(film)
    session.flush()
    session.add(
        FilmAlias(
            film_id=film.id,
            alias_text="برشامة",
            normalized_alias="برشامة",
            alias_language="ar",
            alias_type="seed",
            confidence=1.0,
            source="seed",
        )
    )
    session.commit()

    results = FilmRepository(session).search("Bershama", limit=5)
    assert results
    assert results[0].id == film.id


def test_acquisition_lookup_elcinema_title_specific_fetch_creates_film() -> None:
    # This validates the "gap 2" flow without any network calls by stubbing
    # the elCinema chart + title-boxoffice fetches.
    import src.services.acquisition_lookup as al
    import src.services.acquisition_lookup_job as jobmod

    session = make_session()
    seed_sources(session)

    class DummyElCinemaClient:
        base_url = "https://dummy.elcinema"

        def fetch_boxoffice_chart(self) -> str:
            return ""

        def fetch_title_boxoffice(self, work_id: str) -> str:
            return ""

    my_title = "My Test Film"

    chart_rows = [
        ExtractedRecord(
            source_name="elCinema",
            source_url="https://dummy.elcinema/en/boxoffice",
            source_entity_id="work_101",
            country_code="EG",
            film_title_raw=my_title,
            film_title_ar_raw=None,
            release_year_hint=2024,
            record_scope="title",
            record_granularity="week",
            record_semantics="title_period_gross",
            evidence_type="title_performance",
            period_label_raw="2024-W01",
            period_start_date=date(2024, 1, 1),
            period_end_date=date(2024, 1, 7),
            period_key="2024-W01",
            rank=1,
            period_gross_local=1_000_000.0,
            cumulative_gross_local=2_000_000.0,
            currency="EGP",
            admissions_actual=None,
            parser_confidence=0.95,
            source_confidence=0.90,
            notes="stub chart",
            raw_payload_json={"stub": True},
        )
    ]

    detail_rows = [
        ExtractedRecord(
            source_name="elCinema",
            source_url="https://dummy.elcinema/en/work/work_101/boxoffice",
            source_entity_id="work_101_EG_2024_1",
            country_code="EG",
            film_title_raw=my_title,
            film_title_ar_raw=None,
            release_year_hint=2024,
            record_scope="title",
            record_granularity="week",
            record_semantics="title_period_gross",
            evidence_type="title_performance",
            period_label_raw="2024-W01",
            period_start_date=date(2024, 1, 1),
            period_end_date=date(2024, 1, 7),
            period_key="2024-W01",
            rank=1,
            period_gross_local=1_100_000.0,
            cumulative_gross_local=2_100_000.0,
            currency="EGP",
            admissions_actual=None,
            parser_confidence=0.95,
            source_confidence=0.90,
            notes="stub title boxoffice",
            raw_payload_json={"stub": True},
        )
    ]

    # Stub out all network-y adapters for the staged pipeline.
    jobmod.run_bom_weekly = lambda: []  # type: ignore[assignment]
    jobmod.ElCinemaClient = DummyElCinemaClient  # type: ignore[assignment]
    jobmod.parse_current_chart = lambda html, source_url, chart_limit=25: chart_rows  # type: ignore[assignment]
    jobmod.parse_title_boxoffice = lambda html, source_url, work_id: detail_rows  # type: ignore[assignment]
    jobmod.run_elcinema_title_released_markets = lambda **kwargs: []  # type: ignore[assignment]

    res = al.perform_acquisition_lookup(session, query=my_title, release_year_hint=2024, imdb_title_id=None)
    assert res.resolved_film is not None
    assert res.resolved_film.canonical_title == my_title

    observed_aliases = (
        session.query(FilmAlias)
        .filter(FilmAlias.film_id == res.resolved_film.id, FilmAlias.alias_type == "source_title")
        .all()
    )
    assert observed_aliases


def test_acquisition_lookup_imdb_title_specific_fetch_builds_report() -> None:
    import src.services.acquisition_lookup as al
    import src.services.acquisition_lookup_job as jobmod

    session = make_session()
    seed_sources(session)

    # Avoid any evidence ingestion; only ratings should be inserted.
    jobmod.run_bom_weekly = lambda: []  # type: ignore[assignment]
    jobmod.run_elcinema_title_released_markets = lambda **kwargs: []  # type: ignore[assignment]

    class DummyElCinemaClient:
        base_url = "https://dummy.elcinema"

        def fetch_boxoffice_chart(self) -> str:
            return ""

        def fetch_title_boxoffice(self, work_id: str) -> str:
            return ""

    jobmod.ElCinemaClient = DummyElCinemaClient  # type: ignore[assignment]
    jobmod.parse_current_chart = lambda html, source_url, chart_limit=25: []  # type: ignore[assignment]
    jobmod.parse_title_boxoffice = lambda html, source_url, work_id: []  # type: ignore[assignment]

    my_title = "IMDb Stub Title"
    imdb_id = "tt1234567"

    metric = ImdbMetric(
        source_name="IMDb",
        film_title_raw=my_title,
        rating_value=8.1,
        vote_count=12345,
        popularity_rank=10,
        source_url="https://imdb.test/title/tt1234567",
        payload={"stub": True},
    )
    jobmod.run_imdb_daily = lambda title_id, fallback_title: [metric]  # type: ignore[assignment]

    res = al.perform_acquisition_lookup(session, query=my_title, release_year_hint=2024, imdb_title_id=imdb_id)
    assert res.resolved_film is not None
    assert res.resolved_film.canonical_title == my_title
    assert res.report is not None
    assert session.query(FilmAlias).filter(FilmAlias.film_id == res.resolved_film.id).count() >= 1

    # Ratings should exist (so the score is computed with IMDb sentiment).
    assert session.query(NormalizedEvidence).count() == 0  # no evidence ingestion expected
    # If IMDb ratings inserted correctly, a Film should have rows in ratings_metrics.
    assert session.query(RatingsMetric).filter(RatingsMetric.film_id == res.resolved_film.id).count() >= 1


def test_lookup_job_fast_discovery_queues_review_for_unlinked_evidence() -> None:
    session = make_session()
    seed_sources(session)

    query_title = "Unlinked Evidence Film"

    # Insert title evidence but without any NormalizedEvidence row => film_id is NULL in lookup matches.
    session.add(
        RawEvidence(
            source_run_id=None,
            source_name="elCinema",
            source_url="https://example.test",
            source_entity_id="work_999",
            country_code="EG",
            film_title_raw=query_title,
            film_title_ar_raw=None,
            release_year_hint=2024,
            record_scope="title",
            record_granularity="week",
            record_semantics="title_period_gross",
            evidence_type="title_performance",
            period_label_raw="2024-W01",
            period_start_date=date(2024, 1, 1),
            period_end_date=date(2024, 1, 7),
            period_key="2024-W01",
            rank=1,
            period_gross_local=1_000_000.0,
            cumulative_gross_local=2_000_000.0,
            currency="EGP",
            admissions_actual=None,
            admissions_estimated=None,
            parser_confidence=0.9,
            source_confidence=0.9,
            match_confidence=None,
            notes="test | freshness=fresh",
            raw_payload_json={},
        )
    )
    session.commit()

    view = start_acquisition_lookup_job(
        session,
        query=query_title,
        release_year_hint=2024,
        imdb_title_id=None,
    )

    # Discovery should advance stage (queued enrichment pipeline).
    assert view.job.stage != "discovery"

    # Discovery should enqueue a review item for unlinked evidence.
    open_items = session.query(ReviewQueue).filter(ReviewQueue.status == "open").all()
    assert open_items
    assert open_items[0].reason == "lookup_unlinked_title_evidence"


def test_lookup_job_resume_skips_elcinema_title_lookup_when_coverage_strong() -> None:
    session = make_session()
    seed_sources(session)

    query_title = "Strong Coverage Film"
    # Two elCinema rows that should both match strongly.
    for i in range(2):
        session.add(
            RawEvidence(
                source_run_id=None,
                source_name="elCinema",
                source_url="https://example.test",
                source_entity_id=f"work_{i}",
                country_code="EG",
                film_title_raw=query_title,
                film_title_ar_raw=None,
                release_year_hint=2024,
                record_scope="title",
                record_granularity="week",
                record_semantics="title_period_gross",
                evidence_type="title_performance",
                period_label_raw=f"2024-W0{i+1}",
                period_start_date=date(2024, 1, 1),
                period_end_date=date(2024, 1, 7),
                period_key=f"2024-W0{i+1}",
                rank=1,
                period_gross_local=1_000_000.0,
                cumulative_gross_local=2_000_000.0,
                currency="EGP",
                admissions_actual=None,
                admissions_estimated=None,
                parser_confidence=0.9,
                source_confidence=0.9,
                match_confidence=None,
                notes="test | freshness=fresh",
                raw_payload_json={},
            )
        )
    # Two Box Office Mojo rows also to avoid BOM title lookup network.
    for i in range(2):
        session.add(
            RawEvidence(
                source_run_id=None,
                source_name="Box Office Mojo",
                source_url="https://example.test",
                source_entity_id=f"/release/tt{i}",
                country_code="US",
                film_title_raw=query_title,
                film_title_ar_raw=None,
                release_year_hint=2024,
                record_scope="title",
                record_granularity="weekend",
                record_semantics="title_period_gross",
                evidence_type="title_performance",
                period_label_raw=f"2024W0{i+1}",
                period_start_date=None,
                period_end_date=None,
                period_key=f"2024W0{i+1}",
                rank=1,
                period_gross_local=1_000_000.0,
                cumulative_gross_local=2_000_000.0,
                currency="USD",
                admissions_actual=None,
                admissions_estimated=None,
                parser_confidence=0.9,
                source_confidence=0.9,
                match_confidence=None,
                notes="test | freshness=fresh",
                raw_payload_json={},
            )
        )
    session.commit()

    import src.services.acquisition_lookup_job as jobmod

    # If stage runs unexpectedly, this will raise and fail the test.
    class ExplodingElCinemaClient:
        base_url = "https://dummy.elcinema"

        def __init__(self) -> None:
            raise AssertionError("elCinema client should not be instantiated when coverage is strong.")

    jobmod.ElCinemaClient = ExplodingElCinemaClient  # type: ignore[assignment]
    jobmod.parse_current_chart = lambda *args, **kwargs: []  # type: ignore[assignment]
    jobmod.parse_title_boxoffice = lambda *args, **kwargs: []  # type: ignore[assignment]
    jobmod.run_elcinema_title_released_markets = lambda **kwargs: []  # type: ignore[assignment]
    jobmod.run_bom_weekly = lambda: []  # type: ignore[assignment]

    view = start_acquisition_lookup_job(
        session,
        query=query_title,
        release_year_hint=2024,
        imdb_title_id=None,
    )
    # Resume should skip elCinema title lookup because strong coverage exists.
    view2 = resume_acquisition_lookup_job(session, view.job.id)
    assert "skipping" in (view2.job.notes or "").lower()


def main() -> None:
    # Run everything; failures raise AssertionError.
    test_transliteration_normalized_match()
    test_auto_create_new_film()
    test_dedupe_idempotent()
    test_estimated_admissions_for_gross_only()
    test_acquisition_lookup_evidence_matches()
    test_film_search_cross_language_reranking()
    test_acquisition_lookup_elcinema_title_specific_fetch_creates_film()
    test_acquisition_lookup_imdb_title_specific_fetch_builds_report()
    test_lookup_job_fast_discovery_queues_review_for_unlinked_evidence()
    test_lookup_job_resume_skips_elcinema_title_lookup_when_coverage_strong()
    print("Selftest passed.")


if __name__ == "__main__":
    main()

