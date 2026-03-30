-- Investment Analysis output store (aligned to existing production schema).

CREATE TABLE IF NOT EXISTS film_investment_analysis (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  film_id UUID REFERENCES films(id) ON DELETE CASCADE,
  predicted_first_watch NUMERIC(18, 2),
  suggested_marketing_spend NUMERIC(18, 2),
  roi NUMERIC(18, 4),
  estimated_revenue NUMERIC(18, 2),
  estimated_profit NUMERIC(18, 2),
  decision TEXT,
  model_version TEXT DEFAULT 'v1',
  computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_film_investment_analysis_film
  ON film_investment_analysis(film_id, computed_at DESC);
