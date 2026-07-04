-- v0.6.0_run_digests_v3.sql
-- Adds M47a v3 dossier columns to run_digests.
-- Pre-req: v0.4.0 migration applied (run_digests table exists).

BEGIN;

ALTER TABLE run_digests
    ADD COLUMN IF NOT EXISTS digest_v3                 JSON,
    ADD COLUMN IF NOT EXISTS sector_rotation           JSON,
    ADD COLUMN IF NOT EXISTS chain_dossiers            JSON,
    ADD COLUMN IF NOT EXISTS longitudinal_fund_dossiers JSON;

COMMIT;
