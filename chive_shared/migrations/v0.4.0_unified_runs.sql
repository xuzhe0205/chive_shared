-- v0.4.0_unified_runs.sql
-- Collapses run_intents + runs into one `runs` table; rekeys child tables.
-- Pre-req: pg_dump backup taken. See chive/.db_backups/pre_unified_runs_*.dump
--
-- DESIGN NOTES (refined after live-DB inspection on 2026-06-09):
--   - Historical child rows in run_digests/run_events FK on the OLD
--     runs.run_id (varchar UUID), NOT on runs.id (uuid PK). So during
--     backfill we copy orphan `runs` rows into run_intents using their
--     OLD run_id as the new uuid id — that way the existing 122
--     digests + ~21k events keep linking.
--   - run_intents pre-existing rows keep their `id` as-is. These had
--     zero linked digests/events before this migration (that's the bug
--     we're fixing), so nothing breaks.
--   - run_events has ~29k rows with non-UUID run_id strings
--     ("test", "inspect-stock", etc). These are test/dev artifacts.
--     We NULL them out so the column can be retyped to UUID.
--   - run_digests is fully clean (all run_ids are valid UUIDs). 24
--     digests have no matching `runs` parent — we synthesize stub
--     parent rows for them so ON DELETE CASCADE FK can be added
--     without losing historical digests.
--
-- The script is idempotent (IF EXISTS / IF NOT EXISTS / ON CONFLICT).

-- ── 0. Idempotency guard (psql-level short-circuit) ────────────────────────
-- If run_intents no longer exists, the migration has already run. Exit cleanly.
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'run_intents'
) AS run_intents_exists \gset
\if :run_intents_exists
\echo 'v0.4.0_unified_runs: run_intents exists — applying migration.'
\else
\echo 'v0.4.0_unified_runs: already applied (run_intents gone). No-op.'
\quit
\endif


BEGIN;

-- ── 1. Add missing columns to run_intents (absorb from old runs table) ───────
ALTER TABLE run_intents
  ADD COLUMN IF NOT EXISTS trigger_type      VARCHAR(30),
  ADD COLUMN IF NOT EXISTS session_type      VARCHAR(20),
  ADD COLUMN IF NOT EXISTS universe_size     INTEGER,
  ADD COLUMN IF NOT EXISTS portfolio_size    INTEGER,
  ADD COLUMN IF NOT EXISTS action_item_count INTEGER,
  ADD COLUMN IF NOT EXISTS total_cost_usd    DOUBLE PRECISION DEFAULT 0.0,
  ADD COLUMN IF NOT EXISTS started_at        TIMESTAMP WITH TIME ZONE;


-- ── 2. Backfill: orphan `runs` rows (no matching intent) → run_intents ──────
-- Use the OLD runs.run_id (varchar UUID) as the NEW unified id, because that
-- is what existing run_digests/run_events FK on. Cast varchar(36) → uuid.
-- ON CONFLICT DO NOTHING makes this re-runnable.
INSERT INTO run_intents (
    id, portfolio, status, universe, session_label,
    requested_at, picked_up_at, completed_at, error,
    trigger_type, session_type, started_at,
    universe_size, portfolio_size, action_item_count, total_cost_usd
)
SELECT
    r.run_id::uuid,                              -- preserve link to child tables
    '(unknown)',                                 -- portfolio: historical data has no record
    UPPER(r.status),                             -- normalize lower→upper enum
    NULL,                                        -- universe: not recorded historically
    r.session_type,                              -- repurpose as session_label
    r.started_at,                                -- requested_at ≈ started_at for cron/CLI
    r.started_at,                                -- picked_up_at = started_at (no queue)
    r.completed_at,
    r.error,
    r.trigger_type,
    r.session_type,
    r.started_at,
    r.universe_size,
    r.portfolio_size,
    r.action_item_count,
    r.total_cost_usd
FROM runs r
WHERE NOT EXISTS (
    SELECT 1 FROM run_intents ri WHERE ri.id = r.run_id::uuid
)
ON CONFLICT (id) DO NOTHING;


-- ── 3. Synthesize stub parents for orphan digests ───────────────────────────
-- 24 historical digest rows have a run_id that points to no `runs` row
-- (likely an old test/manual run whose parent was purged). Insert a stub
-- intent so the about-to-be-added FK doesn't reject them. We preserve the
-- digest's history at the cost of marking the parent as session 'orphan'.
INSERT INTO run_intents (
    id, portfolio, status, session_label,
    requested_at, picked_up_at, completed_at,
    trigger_type, session_type, started_at, action_item_count
)
SELECT DISTINCT
    rd.run_id::uuid,
    '(unknown)',
    'COMPLETE',
    'orphan-recovered',
    rd.created_at,
    rd.created_at,
    rd.created_at,
    'orphan-recovered',
    rd.session_type,
    rd.created_at,
    rd.action_item_count
FROM run_digests rd
WHERE NOT EXISTS (
    SELECT 1 FROM run_intents ri WHERE ri.id = rd.run_id::uuid
)
ON CONFLICT (id) DO NOTHING;


-- ── 4. Clean non-UUID run_id values in run_events ───────────────────────────
-- ~29k events have run_id strings like 'test', 'integration-test-run',
-- 'inspect-stock'. The column is nullable; set to NULL so we can ALTER
-- COLUMN TYPE UUID without USING-cast failures. Audit history (agent_name,
-- model, tokens, cost) is preserved — only the orphan run-id pointer is lost.
UPDATE run_events
   SET run_id = NULL
 WHERE run_id IS NOT NULL
   AND run_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';


-- ── 5. Synthesize stub parents for orphan events with valid-UUID run_ids ────
-- ~61 distinct UUIDs in run_events point to no parent. Same treatment as
-- step 3: add a stub intent so the FK can be enforced. Events are
-- attached at the (run, agent_name) grain — losing them would erase
-- token/cost audit trail.
INSERT INTO run_intents (
    id, portfolio, status, session_label,
    requested_at, picked_up_at, completed_at,
    trigger_type, session_type, started_at
)
SELECT DISTINCT
    re.run_id::uuid,
    '(unknown)',
    'COMPLETE',
    'orphan-recovered',
    COALESCE(MIN(re.created_at) OVER (PARTITION BY re.run_id), now()),
    COALESCE(MIN(re.created_at) OVER (PARTITION BY re.run_id), now()),
    COALESCE(MIN(re.created_at) OVER (PARTITION BY re.run_id), now()),
    'orphan-recovered',
    'manual',
    COALESCE(MIN(re.created_at) OVER (PARTITION BY re.run_id), now())
FROM run_events re
WHERE re.run_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM run_intents ri WHERE ri.id = re.run_id::uuid
  )
ON CONFLICT (id) DO NOTHING;


-- ── 6. Drop existing indexes/constraints on child tables that touch run_id ──
ALTER TABLE run_digests DROP CONSTRAINT IF EXISTS run_digests_run_id_fkey;
ALTER TABLE run_events  DROP CONSTRAINT IF EXISTS run_events_run_id_fkey;


-- ── 7. Retype child-table run_id columns from VARCHAR(36) → UUID ────────────
ALTER TABLE run_digests ALTER COLUMN run_id TYPE UUID USING run_id::uuid;
ALTER TABLE run_events  ALTER COLUMN run_id TYPE UUID USING NULLIF(run_id, '')::uuid;


-- ── 8. Drop the old runs table (data already backfilled in step 2) ──────────
DROP TABLE IF EXISTS runs CASCADE;


-- ── 9. Rename run_intents → runs ────────────────────────────────────────────
ALTER TABLE run_intents RENAME TO runs;


-- ── 10. Drop the now-redundant runs.run_id placeholder column ───────────────
ALTER TABLE runs DROP COLUMN IF EXISTS run_id;


-- ── 11. Add FK constraints on child tables → unified runs(id) ───────────────
ALTER TABLE run_digests
  ADD CONSTRAINT run_digests_run_id_fkey
  FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;

ALTER TABLE run_events
  ADD CONSTRAINT run_events_run_id_fkey
  FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;


-- ── 12. Rename indexes for hygiene (idempotent) ─────────────────────────────
ALTER INDEX IF EXISTS run_intents_pkey         RENAME TO runs_pkey;
ALTER INDEX IF EXISTS ix_run_intents_portfolio RENAME TO ix_runs_portfolio;
ALTER INDEX IF EXISTS ix_run_intents_status    RENAME TO ix_runs_status;
DROP INDEX IF EXISTS ix_run_intents_run_id;   -- column is gone; index is redundant


-- ── 13. Rename the status check constraint to match new table name ──────────
ALTER TABLE runs DROP CONSTRAINT IF EXISTS ck_run_intents_status;
ALTER TABLE runs ADD CONSTRAINT ck_runs_status CHECK (
    status IN (
        'PENDING_START', 'STARTING', 'RUNNING', 'COMPLETE', 'FAILED',
        'PENDING_STOP', 'STOPPING', 'CANCELLED',
        'PENDING_FORCE_STOP', 'STALE'
    )
);

COMMIT;

-- ── Verification queries (run manually after commit) ────────────────────────
-- SELECT count(*) FROM runs;
-- SELECT count(*) FROM run_digests rd JOIN runs r ON r.id = rd.run_id;
-- SELECT count(*) FROM run_events  re JOIN runs r ON r.id = re.run_id WHERE re.run_id IS NOT NULL;
-- SELECT count(*) FROM run_digests rd WHERE NOT EXISTS (SELECT 1 FROM runs r WHERE r.id = rd.run_id);
