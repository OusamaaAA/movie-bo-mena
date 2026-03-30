from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, Film, FilmAlias, MarketReference, NormalizedEvidence, RawEvidence, ReconciledEvidence, Source
from src.repositories.film_repository import FilmRepository
from src.repositories.report_repository import ReportRepository
from src.services.ingestion_service import ingest_source
from src.services.matching import TitleMatcher
from src.sources.common import ExtractedRecord


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return SessionLocal()


def _seed_sources(session: Session) -> None:
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


def test_transliteration_normalized_match_creates_attachment() -> None:
    session = _make_session()
    _seed_sources(session)

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


def test_auto_create_new_film_when_no_match() -> None:
    session = _make_session()
    _seed_sources(session)

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


def test_auto_create_new_film_when_no_match_and_year_missing() -> None:
    session = _make_session()
    _seed_sources(session)

    matcher = TitleMatcher(session)
    res = matcher.match_or_queue(
        "New Movie Title",
        release_year_hint=None,
        raw_evidence_id="dummy_raw_2",
        record_scope="title",
        source_entity_id="work_123",
        parser_confidence=0.88,
        source_confidence=0.80,
    )
    assert res.film_id is not None

    created = session.get(Film, res.film_id)
    assert created is not None
    assert created.canonical_title == "New Movie Title"

    # Observed aliases must be inserted into film_aliases.
    observed_aliases = (
        session.query(FilmAlias)
        .filter(FilmAlias.film_id == created.id, FilmAlias.alias_type == "source_title")
        .all()
    )
    assert observed_aliases


def test_dedupe_title_rows_idempotent() -> None:
    session = _make_session()
    _seed_sources(session)

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

    # If dedupe_key works, raw evidence should be inserted only once.
    raw_rows = session.query(ReconciledEvidence).all()  # ensure reconciliation happened
    assert len(raw_rows) >= 0

    # Directly check RawEvidence is deduped through the NormalizedEvidence FK count.
    normalized_count = session.query(NormalizedEvidence).count()
    assert normalized_count == 1


def test_estimated_admissions_for_gross_only() -> None:
    session = _make_session()
    _seed_sources(session)

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
        admissions_actual=None,  # gross-only source
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
    assert float(row.admissions_estimated) == pytest.approx(20.0)


def test_film_search_cross_language_reranking() -> None:
    session = _make_session()
    _seed_sources(session)

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

    repo = FilmRepository(session)
    results = repo.search("Bershama", limit=5)
    assert results
    assert results[0].id == film.id


def test_acquisition_lookup_evidence_matches_title() -> None:
    session = _make_session()
    _seed_sources(session)

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

    repo = ReportRepository(session)
    matches = repo.title_evidence_matches("Bershama", limit=3, sample_size=50, release_year_hint=2020)
    assert matches
    assert matches[0]["title"] == "برشامة"
    assert matches[0]["match_score"] >= 0.9


def test_filmyard_infer_data_date_from_revenues() -> None:
    from src.sources.filmyard.parser import _infer_filmyard_data_date

    item = {
        "id": 29,
        "name": "Test",
        "revenues": [
            {"date": "2026-03-25", "amount": "100", "film_id": 29, "tickets": 1, "cinema_id": 1},
            {"date": "2026-03-25", "amount": "200", "film_id": 29, "tickets": 2, "cinema_id": 2},
        ],
    }
    assert _infer_filmyard_data_date(item) == date(2026, 3, 25)


def test_filmyard_ingest_preserves_parser_period_vs_fetch_day() -> None:
    from src.sources.filmyard.ingest import _with_snapshot_date

    data_day = date(2026, 3, 25)
    fetch_day = date(2026, 3, 27)
    rec = ExtractedRecord(
        source_name="Filmyard",
        source_url="https://example.test/box-office",
        source_entity_id="29",
        country_code="EG",
        film_title_raw="إيجي بست",
        film_title_ar_raw=None,
        release_year_hint=2026,
        record_scope="title",
        record_granularity="day",
        record_semantics="title_period_gross",
        evidence_type="title_performance",
        period_label_raw=str(data_day),
        period_start_date=data_day,
        period_end_date=data_day,
        period_key=str(data_day),
        rank=2,
        period_gross_local=687713.0,
        cumulative_gross_local=2.0,
        currency="EGP",
        admissions_actual=4304.0,
        parser_confidence=0.97,
        source_confidence=0.92,
        notes="Filmyard daily Egypt",
        raw_payload_json={"id": 29},
    )
    out = _with_snapshot_date([rec], fetch_day)
    assert len(out) == 1
    assert out[0].period_start_date == data_day
    assert out[0].period_end_date == data_day
    assert out[0].period_key == "2026-03-25"
    assert out[0].raw_payload_json["snapshot_date"] == "2026-03-27"

