-- Idempotent: adds admissions columns expected by src/models.py (NormalizedEvidence, ReconciledEvidence).
-- Use when you see: column "admissions_actual" of relation "normalized_evidence" does not exist
-- (usually means migration 003 was not applied).

ALTER TABLE normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE normalized_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);

ALTER TABLE reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_actual NUMERIC(18,2);

ALTER TABLE reconciled_evidence
  ADD COLUMN IF NOT EXISTS admissions_estimated NUMERIC(18,2);
