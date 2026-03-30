-- Evidence idempotency + admissions (actual vs estimated)

ALTER TABLE raw_evidence
  ADD COLUMN IF NOT EXISTS dedupe_key TEXT;

-- Unique index allows multiple NULLs but blocks duplicate title-level rows.
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_evidence_dedupe_key
  ON raw_evidence(dedupe_key);

ALTER TABLE raw_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

ALTER TABLE normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

ALTER TABLE reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

