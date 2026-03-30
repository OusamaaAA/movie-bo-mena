-- Intelligence engine: DB-side computation for film_performance_features.
-- Requires migration 007 (table film_performance_features).
-- Additive only — creates functions; does not alter existing tables.
--
-- When to run (application / ops):
--   • After a film report is built (app calls compute_film_performance_features via Python).
--   • After bulk reconciled_evidence changes: run compute_all_film_performance_features() or per-film.
--   • Manual refresh: SELECT compute_film_performance_features('<uuid>'); or compute_all_film_performance_features();

CREATE OR REPLACE FUNCTION _fpf_one_market(p_film_id uuid, p_country text)
RETURNS TABLE (
  opening_admissions numeric,
  peak_admissions numeric,
  total_admissions numeric,
  weeks int,
  run_shape text,
  stability numeric,
  peak_week_index int
)
LANGUAGE plpgsql
STABLE
AS $function$
DECLARE
  a numeric[];
  n int;
  i int;
  peak_val numeric;
  pidx int;
  j int;
  ret_sum numeric := 0;
  ret_cnt int := 0;
  rr numeric;
  stab numeric;
  shape text;
BEGIN
  SELECT array_agg(s.adm ORDER BY s.period_start_date NULLS LAST, s.period_key NULLS LAST)
  INTO a
  FROM (
    SELECT
      re.period_start_date,
      re.period_key,
      CASE
        WHEN re.admissions_actual IS NOT NULL AND re.admissions_actual > 0 THEN re.admissions_actual::numeric
        WHEN re.admissions_estimated IS NOT NULL AND re.admissions_estimated > 0 THEN re.admissions_estimated::numeric
        ELSE NULL
      END AS adm
    FROM reconciled_evidence re
    WHERE re.film_id = p_film_id
      AND re.country_code = p_country
  ) s
  WHERE s.adm IS NOT NULL;

  IF a IS NULL OR coalesce(array_length(a, 1), 0) = 0 THEN
    RETURN QUERY SELECT
      NULL::numeric, NULL::numeric, NULL::numeric, 0, NULL::text, NULL::numeric, NULL::int;
    RETURN;
  END IF;

  n := array_length(a, 1);
  peak_val := (SELECT max(x) FROM unnest(a) AS x);
  pidx := NULL;
  FOR i IN 1..n LOOP
    IF a[i] = peak_val THEN
      pidx := i;
      EXIT;
    END IF;
  END LOOP;

  IF pidx IS NULL THEN
    pidx := 1;
  END IF;

  IF pidx < n THEN
    FOR j IN (pidx + 1)..n LOOP
      IF a[j - 1] > 0 THEN
        rr := a[j] / a[j - 1];
        ret_sum := ret_sum + rr;
        ret_cnt := ret_cnt + 1;
      END IF;
    END LOOP;
  END IF;

  IF ret_cnt > 0 THEN
    stab := ret_sum / ret_cnt;
  ELSE
    stab := NULL;
  END IF;

  IF pidx = 1 THEN
    shape := 'front_loaded';
  ELSIF pidx IN (2, 3) THEN
    shape := 'build_then_decay';
  ELSIF n >= 10 AND stab IS NOT NULL AND stab > 0.6 THEN
    shape := 'long_tail';
  ELSE
    shape := 'standard_decay';
  END IF;

  RETURN QUERY SELECT
    a[1],
    peak_val,
    (SELECT coalesce(sum(x), 0) FROM unnest(a) AS x),
    n,
    shape,
    stab,
    pidx;
END;
$function$;


CREATE OR REPLACE FUNCTION compute_film_performance_features(p_film_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  eg record;
  sa record;
  ae record;
  mena numeric;
  eg_t numeric;
  sa_t numeric;
  ae_t numeric;
  v_imdb numeric(6, 3);
  v_elc numeric(6, 3);
  v_lb_rating numeric(6, 3);
  v_lb_votes int;
BEGIN
  SELECT * INTO eg FROM _fpf_one_market(p_film_id, 'EG') LIMIT 1;
  SELECT * INTO sa FROM _fpf_one_market(p_film_id, 'SA') LIMIT 1;
  SELECT * INTO ae FROM _fpf_one_market(p_film_id, 'AE') LIMIT 1;

  eg_t := coalesce(eg.total_admissions, 0);
  sa_t := coalesce(sa.total_admissions, 0);
  ae_t := coalesce(ae.total_admissions, 0);
  mena := eg_t + sa_t + ae_t;

  SELECT rm.rating_value::numeric(6, 3)
  INTO v_imdb
  FROM ratings_metrics rm
  WHERE rm.film_id = p_film_id
    AND upper(trim(rm.source_name)) = 'IMDB'
  ORDER BY rm.metric_date DESC
  LIMIT 1;

  IF v_imdb IS NULL THEN
    SELECT rm.rating_value::numeric(6, 3)
    INTO v_imdb
    FROM ratings_metrics rm
    WHERE rm.film_id = p_film_id
      AND rm.source_name = 'IMDb'
    ORDER BY rm.metric_date DESC
    LIMIT 1;
  END IF;

  SELECT rm.rating_value::numeric(6, 3)
  INTO v_elc
  FROM ratings_metrics rm
  WHERE rm.film_id = p_film_id
    AND upper(trim(rm.source_name)) = 'ELCINEMA'
  ORDER BY rm.metric_date DESC
  LIMIT 1;

  IF v_elc IS NULL THEN
    SELECT rm.rating_value::numeric(6, 3)
    INTO v_elc
    FROM ratings_metrics rm
    WHERE rm.film_id = p_film_id
      AND rm.source_name = 'elCinema'
    ORDER BY rm.metric_date DESC
    LIMIT 1;
  END IF;

  SELECT rm.rating_value::numeric(6, 3), rm.vote_count
  INTO v_lb_rating, v_lb_votes
  FROM ratings_metrics rm
  WHERE rm.film_id = p_film_id
    AND upper(trim(rm.source_name)) = 'LETTERBOXD'
  ORDER BY rm.metric_date DESC
  LIMIT 1;

  INSERT INTO film_performance_features (
    film_id,
    eg_opening_admissions,
    eg_peak_admissions,
    eg_total_admissions,
    eg_weeks,
    eg_run_shape,
    eg_stability,
    sa_opening_admissions,
    sa_peak_admissions,
    sa_total_admissions,
    sa_weeks,
    sa_run_shape,
    sa_stability,
    ae_opening_admissions,
    ae_peak_admissions,
    ae_total_admissions,
    ae_periods,
    ae_run_shape,
    ae_stability,
    mena_total_admissions,
    eg_share,
    sa_share,
    ae_share,
    imdb_rating,
    elcinema_rating,
    letterboxd_rating,
    letterboxd_votes,
    last_computed_at
  )
  VALUES (
    p_film_id,
    eg.opening_admissions,
    eg.peak_admissions,
    eg.total_admissions,
    eg.weeks,
    eg.run_shape,
    eg.stability,
    sa.opening_admissions,
    sa.peak_admissions,
    sa.total_admissions,
    sa.weeks,
    sa.run_shape,
    sa.stability,
    ae.opening_admissions,
    ae.peak_admissions,
    ae.total_admissions,
    ae.weeks,
    ae.run_shape,
    ae.stability,
    CASE WHEN mena > 0 THEN mena ELSE NULL END,
    CASE WHEN mena > 0 THEN eg_t / mena ELSE NULL END,
    CASE WHEN mena > 0 THEN sa_t / mena ELSE NULL END,
    CASE WHEN mena > 0 THEN ae_t / mena ELSE NULL END,
    v_imdb,
    v_elc,
    v_lb_rating,
    v_lb_votes,
    now()
  )
  ON CONFLICT (film_id) DO UPDATE SET
    eg_opening_admissions = EXCLUDED.eg_opening_admissions,
    eg_peak_admissions = EXCLUDED.eg_peak_admissions,
    eg_total_admissions = EXCLUDED.eg_total_admissions,
    eg_weeks = EXCLUDED.eg_weeks,
    eg_run_shape = EXCLUDED.eg_run_shape,
    eg_stability = EXCLUDED.eg_stability,
    sa_opening_admissions = EXCLUDED.sa_opening_admissions,
    sa_peak_admissions = EXCLUDED.sa_peak_admissions,
    sa_total_admissions = EXCLUDED.sa_total_admissions,
    sa_weeks = EXCLUDED.sa_weeks,
    sa_run_shape = EXCLUDED.sa_run_shape,
    sa_stability = EXCLUDED.sa_stability,
    ae_opening_admissions = EXCLUDED.ae_opening_admissions,
    ae_peak_admissions = EXCLUDED.ae_peak_admissions,
    ae_total_admissions = EXCLUDED.ae_total_admissions,
    ae_periods = EXCLUDED.ae_periods,
    ae_run_shape = EXCLUDED.ae_run_shape,
    ae_stability = EXCLUDED.ae_stability,
    mena_total_admissions = EXCLUDED.mena_total_admissions,
    eg_share = EXCLUDED.eg_share,
    sa_share = EXCLUDED.sa_share,
    ae_share = EXCLUDED.ae_share,
    imdb_rating = EXCLUDED.imdb_rating,
    elcinema_rating = EXCLUDED.elcinema_rating,
    letterboxd_rating = EXCLUDED.letterboxd_rating,
    letterboxd_votes = EXCLUDED.letterboxd_votes,
    last_computed_at = EXCLUDED.last_computed_at;
END;
$function$;


CREATE OR REPLACE FUNCTION compute_all_film_performance_features()
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT f.id FROM films AS f
  LOOP
    PERFORM compute_film_performance_features(r.id);
  END LOOP;
END;
$function$;
