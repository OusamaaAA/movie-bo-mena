from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Film(Base):
    __tablename__ = "films"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    canonical_title: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_title_ar: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_title: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    release_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    identity_confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    aliases: Mapped[list["FilmAlias"]] = relationship(back_populates="film", cascade="all, delete-orphan")


class FilmAlias(Base):
    __tablename__ = "film_aliases"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    alias_text: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_alias: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    alias_language: Mapped[str | None] = mapped_column(String(8), nullable=True)
    alias_type: Mapped[str] = mapped_column(String(32), default="title")
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=1.0)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    needs_review: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    film: Mapped[Film] = relationship(back_populates="aliases")


class Source(Base):
    __tablename__ = "sources"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    source_code: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_family: Mapped[str] = mapped_column(String(64), nullable=False)
    schedule_type: Mapped[str] = mapped_column(String(16), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class SourceRun(Base):
    __tablename__ = "source_runs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"), nullable=True)
    run_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_count: Mapped[int] = mapped_column(Integer, default=0)
    normalized_count: Mapped[int] = mapped_column(Integer, default=0)
    reconciled_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class RawEvidence(Base):
    __tablename__ = "raw_evidence"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    source_run_id: Mapped[str | None] = mapped_column(ForeignKey("source_runs.id"), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    country_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    film_title_raw: Mapped[str] = mapped_column(Text, nullable=False)
    film_title_ar_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    release_year_hint: Mapped[int | None] = mapped_column(Integer, nullable=True)
    record_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    record_granularity: Mapped[str] = mapped_column(String(32), nullable=False)
    record_semantics: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(32), nullable=False)
    period_label_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    period_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    period_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    cumulative_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    admissions_actual: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    admissions_estimated: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    parser_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    source_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    match_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class NormalizedEvidence(Base):
    __tablename__ = "normalized_evidence"
    __table_args__ = (UniqueConstraint("raw_evidence_id"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    raw_evidence_id: Mapped[str] = mapped_column(ForeignKey("raw_evidence.id", ondelete="CASCADE"), nullable=False)
    film_id: Mapped[str | None] = mapped_column(ForeignKey("films.id", ondelete="SET NULL"), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    country_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    record_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    record_granularity: Mapped[str] = mapped_column(String(32), nullable=False)
    record_semantics: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(32), nullable=False)
    period_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    period_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    cumulative_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    admissions_actual: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    admissions_estimated: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    parser_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    source_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    match_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    normalized_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class ReconciledEvidence(Base):
    __tablename__ = "reconciled_evidence"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    source_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    country_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    record_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    record_granularity: Mapped[str] = mapped_column(String(32), nullable=False)
    record_semantics: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(32), nullable=False)
    period_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    period_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    cumulative_gross_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    admissions_actual: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    admissions_estimated: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    winning_source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    contributing_sources: Mapped[list[str]] = mapped_column(JSON, default=list)
    explanation: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class ReviewQueue(Base):
    __tablename__ = "review_queue"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    raw_evidence_id: Mapped[str | None] = mapped_column(ForeignKey("raw_evidence.id", ondelete="SET NULL"), nullable=True)
    film_title_raw: Mapped[str] = mapped_column(Text, nullable=False)
    release_year_hint: Mapped[int | None] = mapped_column(Integer, nullable=True)
    candidate_film_id: Mapped[str | None] = mapped_column(ForeignKey("films.id", ondelete="SET NULL"), nullable=True)
    candidate_score: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="open")
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    analyst_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RatingsMetric(Base):
    __tablename__ = "ratings_metrics"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    rating_value: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    vote_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    popularity_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metric_date: Mapped[date] = mapped_column(Date, nullable=False)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class MarketingInput(Base):
    __tablename__ = "marketing_inputs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    market_code: Mapped[str] = mapped_column(String(8), nullable=False)
    spend_local: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    spend_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    campaign_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    campaign_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    channel_mix_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class OutcomeTarget(Base):
    __tablename__ = "outcome_targets"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=False)
    market_code: Mapped[str] = mapped_column(String(8), nullable=False)
    target_label: Mapped[str] = mapped_column(String(64), default="first_watch_target")
    target_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    target_unit: Mapped[str] = mapped_column(String(32), default="admissions")
    period_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class FilmPerformanceFeatures(Base):
    """One row per film: aggregates from reconciled_evidence + ratings_metrics (recomputed with film report)."""

    __tablename__ = "film_performance_features"
    film_id: Mapped[str] = mapped_column(String(36), ForeignKey("films.id", ondelete="CASCADE"), primary_key=True)

    eg_opening_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    eg_peak_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    eg_total_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    eg_weeks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    eg_run_shape: Mapped[str | None] = mapped_column(Text, nullable=True)
    eg_stability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)

    sa_opening_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    sa_peak_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    sa_total_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    sa_weeks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sa_run_shape: Mapped[str | None] = mapped_column(Text, nullable=True)
    sa_stability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)

    ae_opening_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    ae_peak_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    ae_total_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    ae_periods: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ae_run_shape: Mapped[str | None] = mapped_column(Text, nullable=True)
    ae_stability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)

    mena_total_admissions: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    eg_share: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    sa_share: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    ae_share: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)

    imdb_rating: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    elcinema_rating: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    letterboxd_rating: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    letterboxd_votes: Mapped[int | None] = mapped_column(Integer, nullable=True)

    last_computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FilmInvestmentAnalysis(Base):
    __tablename__ = "film_investment_analysis"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    film_id: Mapped[str | None] = mapped_column(ForeignKey("films.id", ondelete="CASCADE"), nullable=True, index=True)
    predicted_first_watch: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    suggested_marketing_spend: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    roi: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    estimated_revenue: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    estimated_profit: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_version: Mapped[str | None] = mapped_column(Text, default="v1")
    computed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class MarketReference(Base):
    __tablename__ = "market_reference"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    market_code: Mapped[str] = mapped_column(String(8), nullable=False)
    reference_type: Mapped[str] = mapped_column(String(64), nullable=False)
    value_num: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    period_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class LookupJob(Base):
    __tablename__ = "lookup_jobs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    release_year_hint: Mapped[int | None] = mapped_column(Integer, nullable=True)
    imdb_title_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Human-readable state for UI and resumability.
    status: Mapped[str] = mapped_column(String(32), default="queued")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    stage: Mapped[str] = mapped_column(String(32), default="discovery")

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    # JSON blobs to avoid schema churn during iterative workflows.
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    warnings_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    coverage_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    fast_matches_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    context_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    resolved_film_id: Mapped[str | None] = mapped_column(ForeignKey("films.id", ondelete="SET NULL"), nullable=True)


class BulkLookupBatch(Base):
    __tablename__ = "bulk_lookup_batches"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    status: Mapped[str] = mapped_column(String(16), default="running")  # running, paused, completed
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    processed_items: Mapped[int] = mapped_column(Integer, default=0)
    success_items: Mapped[int] = mapped_column(Integer, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class BulkLookupItem(Base):
    __tablename__ = "bulk_lookup_items"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    batch_id: Mapped[str] = mapped_column(ForeignKey("bulk_lookup_batches.id", ondelete="CASCADE"), nullable=False, index=True)
    queue_index: Mapped[int] = mapped_column(Integer, nullable=False)
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    release_year_hint: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="queued")  # queued, running, completed, failed, retryable
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    lookup_job_id: Mapped[str | None] = mapped_column(ForeignKey("lookup_jobs.id", ondelete="SET NULL"), nullable=True)
    resolved_film_id: Mapped[str | None] = mapped_column(ForeignKey("films.id", ondelete="SET NULL"), nullable=True)
    matched_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    coverage_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    ratings_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

