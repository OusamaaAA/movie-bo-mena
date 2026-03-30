from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, LookupJob, Source
from src.services import acquisition_lookup_job as jobmod
from src.services.json_utils import make_json_safe


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


def test_make_json_safe_nested_uuid_datetime_decimal() -> None:
    u = uuid4()
    value = {
        "id": u,
        "when": datetime(2026, 3, 25, 1, 2, 3),
        "d": date(2026, 3, 25),
        "n": Decimal("12.34"),
        "list": [{"inner": u, "nums": {1, 2, 3}}],
    }
    out = make_json_safe(value)
    assert out["id"] == str(u)
    assert out["when"] == "2026-03-25T01:02:03"
    assert out["d"] == "2026-03-25"
    assert out["n"] == 12.34
    assert out["list"][0]["inner"] == str(u)


def test_lookup_job_json_fields_sanitize_uuid_values(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    film_uuid = uuid4()
    monkeypatch.setattr(
        jobmod,
        "_compute_fast_matches",
        lambda *_args, **_kwargs: [{"film_id": film_uuid, "match_score": 0.93, "title": "Safe Title", "source_name": "elCinema"}],
    )
    view = jobmod.start_acquisition_lookup_job(s, query="Safe Title", release_year_hint=2026, imdb_title_id=None)
    persisted = s.get(LookupJob, view.job.id)
    assert isinstance(persisted.fast_matches_json, list)
    assert persisted.fast_matches_json[0]["film_id"] == str(film_uuid)
    assert isinstance(UUID(persisted.fast_matches_json[0]["film_id"]), UUID)


def test_lookup_job_exception_path_warning_is_json_safe(monkeypatch) -> None:
    s = _session()
    _seed_sources(s)
    monkeypatch.setattr(jobmod, "_compute_fast_matches", lambda *_args, **_kwargs: [])

    def _boom(*_args, **_kwargs):
        raise RuntimeError({"bad_uuid": uuid4()})

    monkeypatch.setattr(jobmod, "discover_elcinema_candidates", _boom)
    job = LookupJob(query_text="Err Title", release_year_hint=2026, imdb_title_id=None, status="running", is_active=True, stage=jobmod.STAGE_DISCOVERY)
    s.add(job)
    s.flush()
    s.commit()
    jobmod._run_lookup_job_stage(s, job.id, max_stage=jobmod.STAGE_DISCOVERY)
    failed = s.get(LookupJob, job.id)
    warnings = (failed.warnings_json or {}).get("warnings") or []
    assert warnings
    w = warnings[-1]
    assert w.get("type") == "lookup_failed_exception"
    assert isinstance(w.get("message"), str)
    assert isinstance(w.get("exception_class"), str)
