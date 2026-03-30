CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS films (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  canonical_title TEXT NOT NULL,
  canonical_title_ar TEXT,
  normalized_title TEXT NOT NULL,
  release_year INT,
  identity_confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_films_normalized_year
  ON films (normalized_title, COALESCE(release_year, 0));

CREATE TABLE IF NOT EXISTS film_aliases (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  alias_text TEXT NOT NULL,
  normalized_alias TEXT NOT NULL,
  alias_language TEXT,
  alias_type TEXT NOT NULL DEFAULT 'title',
  confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0,
  source TEXT,
  needs_review BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_film_aliases_norm ON film_aliases(normalized_alias);

CREATE TABLE IF NOT EXISTS sources (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_code TEXT NOT NULL UNIQUE,
  source_name TEXT NOT NULL,
  source_family TEXT NOT NULL,
  schedule_type TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_runs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES sources(id),
  run_type TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  fetched_count INT NOT NULL DEFAULT 0,
  normalized_count INT NOT NULL DEFAULT 0,
  reconciled_count INT NOT NULL DEFAULT 0,
  error_count INT NOT NULL DEFAULT 0,
  error_message TEXT,
  context_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_source_runs_started ON source_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS raw_evidence (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_run_id UUID REFERENCES source_runs(id) ON DELETE SET NULL,
  source_name TEXT NOT NULL,
  source_url TEXT,
  source_entity_id TEXT,
  country_code TEXT,
  film_title_raw TEXT NOT NULL,
  film_title_ar_raw TEXT,
  release_year_hint INT,
  record_scope TEXT NOT NULL,
  record_granularity TEXT NOT NULL,
  record_semantics TEXT NOT NULL,
  evidence_type TEXT NOT NULL,
  period_label_raw TEXT,
  period_start_date DATE,
  period_end_date DATE,
  period_key TEXT,
  rank INT,
  period_gross_local NUMERIC(18,2),
  cumulative_gross_local NUMERIC(18,2),
  currency TEXT,
  admissions_actual NUMERIC(18,2),
  parser_confidence NUMERIC(5,4),
  source_confidence NUMERIC(5,4),
  match_confidence NUMERIC(5,4),
  notes TEXT,
  raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_raw_evidence_source ON raw_evidence(source_name, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_evidence_period ON raw_evidence(period_key);

CREATE TABLE IF NOT EXISTS normalized_evidence (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  raw_evidence_id UUID NOT NULL UNIQUE REFERENCES raw_evidence(id) ON DELETE CASCADE,
  film_id UUID REFERENCES films(id) ON DELETE SET NULL,
  source_name TEXT NOT NULL,
  country_code TEXT,
  record_scope TEXT NOT NULL,
  record_granularity TEXT NOT NULL,
  record_semantics TEXT NOT NULL,
  evidence_type TEXT NOT NULL,
  period_start_date DATE,
  period_end_date DATE,
  period_key TEXT,
  period_gross_local NUMERIC(18,2),
  cumulative_gross_local NUMERIC(18,2),
  currency TEXT,
  rank INT,
  parser_confidence NUMERIC(5,4),
  source_confidence NUMERIC(5,4),
  match_confidence NUMERIC(5,4),
  normalized_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_normalized_film ON normalized_evidence(film_id, source_name);

CREATE TABLE IF NOT EXISTS reconciled_evidence (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  source_fingerprint TEXT NOT NULL,
  country_code TEXT,
  record_scope TEXT NOT NULL,
  record_granularity TEXT NOT NULL,
  record_semantics TEXT NOT NULL,
  evidence_type TEXT NOT NULL,
  period_start_date DATE,
  period_end_date DATE,
  period_key TEXT,
  rank INT,
  period_gross_local NUMERIC(18,2),
  cumulative_gross_local NUMERIC(18,2),
  currency TEXT,
  winning_source_name TEXT NOT NULL,
  contributing_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
  explanation TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_reconciled_film ON reconciled_evidence(film_id, period_start_date DESC);

CREATE TABLE IF NOT EXISTS review_queue (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  raw_evidence_id UUID REFERENCES raw_evidence(id) ON DELETE SET NULL,
  film_title_raw TEXT NOT NULL,
  release_year_hint INT,
  candidate_film_id UUID REFERENCES films(id) ON DELETE SET NULL,
  candidate_score NUMERIC(5,4),
  status TEXT NOT NULL DEFAULT 'open',
  reason TEXT NOT NULL,
  analyst_notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_review_queue_status ON review_queue(status, created_at DESC);

CREATE TABLE IF NOT EXISTS ratings_metrics (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  source_name TEXT NOT NULL,
  rating_value NUMERIC(6,3),
  vote_count INT,
  popularity_rank INT,
  metric_date DATE NOT NULL,
  raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_ratings_film_date ON ratings_metrics(film_id, metric_date DESC);

CREATE TABLE IF NOT EXISTS marketing_inputs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  market_code TEXT NOT NULL,
  spend_local NUMERIC(18,2),
  spend_currency TEXT,
  campaign_start_date DATE,
  campaign_end_date DATE,
  channel_mix_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_marketing_film_market ON marketing_inputs(film_id, market_code);

CREATE TABLE IF NOT EXISTS outcome_targets (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  market_code TEXT NOT NULL,
  target_label TEXT NOT NULL DEFAULT 'first_watch_target',
  target_value NUMERIC(18,2),
  target_unit TEXT NOT NULL DEFAULT 'admissions',
  period_start_date DATE,
  period_end_date DATE,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_outcome_targets_film ON outcome_targets(film_id, market_code);

CREATE TABLE IF NOT EXISTS market_reference (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  market_code TEXT NOT NULL,
  reference_type TEXT NOT NULL,
  value_num NUMERIC(18,4),
  value_text TEXT,
  period_key TEXT,
  source_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS report_cache (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  report_type TEXT NOT NULL DEFAULT 'film_report',
  cache_key TEXT NOT NULL,
  report_json JSONB NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_report_cache_key ON report_cache(cache_key);

