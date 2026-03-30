-- Paste this entire file into Supabase → SQL Editor → Run (not a file path).
-- Schema: public (default). If your tables live elsewhere, change public. to your schema.
--
-- If CREATE UNIQUE INDEX fails ("duplicate key"), you have duplicate non-null dedupe_key
-- rows. Fix or delete duplicates first, or skip Block A and only run Block B from
-- migrations/005_normalized_reconciled_admissions_columns.sql

-- --- Block A: raw_evidence ---
ALTER TABLE public.raw_evidence
  ADD COLUMN IF NOT EXISTS dedupe_key TEXT;

-- Blocks duplicate non-null dedupe keys; multiple NULLs are still allowed.
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_evidence_dedupe_key
  ON public.raw_evidence (dedupe_key);

ALTER TABLE public.raw_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

-- --- Block B: normalized + reconciled (fixes ORM INSERT on admissions_actual) ---
ALTER TABLE public.normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE public.normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

ALTER TABLE public.reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE public.reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);
