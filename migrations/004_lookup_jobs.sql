-- Acquisition lookup job model (staged / resumable workflow)

CREATE TABLE IF NOT EXISTS lookup_jobs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  query_text TEXT NOT NULL,
  release_year_hint INTEGER,
  imdb_title_id TEXT,

  status TEXT NOT NULL DEFAULT 'queued',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  stage TEXT NOT NULL DEFAULT 'discovery',

  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  notes TEXT,
  warnings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  coverage_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  fast_matches_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  context_json JSONB NOT NULL DEFAULT '{}'::jsonb,

  resolved_film_id UUID REFERENCES films(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_lookup_jobs_active_stage ON lookup_jobs(is_active, stage);

