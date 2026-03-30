-- Intelligence layer: aggregated performance features per film (from reconciled_evidence + ratings).
-- Safe additive migration — does not alter existing tables.

CREATE TABLE IF NOT EXISTS film_performance_features (
  film_id UUID PRIMARY KEY REFERENCES films(id) ON DELETE CASCADE,

  eg_opening_admissions NUMERIC(18, 2),
  eg_peak_admissions NUMERIC(18, 2),
  eg_total_admissions NUMERIC(18, 2),
  eg_weeks INT,
  eg_run_shape TEXT,
  eg_stability NUMERIC(10, 6),

  sa_opening_admissions NUMERIC(18, 2),
  sa_peak_admissions NUMERIC(18, 2),
  sa_total_admissions NUMERIC(18, 2),
  sa_weeks INT,
  sa_run_shape TEXT,
  sa_stability NUMERIC(10, 6),

  ae_opening_admissions NUMERIC(18, 2),
  ae_peak_admissions NUMERIC(18, 2),
  ae_total_admissions NUMERIC(18, 2),
  ae_periods INT,
  ae_run_shape TEXT,
  ae_stability NUMERIC(10, 6),

  mena_total_admissions NUMERIC(18, 2),
  eg_share NUMERIC(10, 6),
  sa_share NUMERIC(10, 6),
  ae_share NUMERIC(10, 6),

  imdb_rating NUMERIC(6, 3),
  elcinema_rating NUMERIC(6, 3),
  letterboxd_rating NUMERIC(6, 3),
  letterboxd_votes INT,

  last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_film_performance_features_computed
  ON film_performance_features(last_computed_at DESC);

DROP VIEW IF EXISTS marketing_model_dataset;

CREATE VIEW marketing_model_dataset AS
SELECT
  f.film_id,
  f.eg_total_admissions,
  f.sa_total_admissions,
  f.ae_total_admissions,
  f.mena_total_admissions,
  f.eg_run_shape,
  f.sa_run_shape,
  f.ae_run_shape,
  f.imdb_rating,
  f.elcinema_rating,
  f.letterboxd_rating,
  f.letterboxd_votes,
  COALESCE(mi.spend_sum, 0)::NUMERIC(18, 2) AS total_marketing_spend,
  COALESCE(ot.fw_sum, 0)::NUMERIC(18, 2) AS total_first_watch
FROM film_performance_features f
LEFT JOIN (
  SELECT film_id, SUM(spend_local) AS spend_sum
  FROM marketing_inputs
  GROUP BY film_id
) mi ON mi.film_id = f.film_id
LEFT JOIN (
  SELECT film_id, SUM(target_value) AS fw_sum
  FROM outcome_targets
  WHERE target_label IN ('first_watch', 'first_watch_target')
  GROUP BY film_id
) ot ON ot.film_id = f.film_id;
