"""
storage/repository.py
──────────────────────
Repository classes — all DB interaction goes through here.

Each repository is scoped to an AsyncSession injected at construction time.
Callers obtain a session via storage.db.get_session() and pass it in.

Repositories:
  RunRepository            — pipeline run lifecycle (Run; replaces v0.3 split
                              of RunRecord + RunIntent)
  AgentArtifactRepository  — per-agent outputs + vector embeddings (AgentArtifact)
  RunEventRepository       — AuditLogger write path (RunEvent)
  RunDigestRepository      — serialised FinalRecommendation (RunDigestRecord)

Artifact lifecycle (two-tier RAG memory):
  raw_report       — full output JSON, 3072-dim embedding; kept for 90 days
  archived_summary — quarter-anchored compressed summary; kept indefinitely

  At 90 days: RAGArchiver (storage/rag.py) calls get_stale_raw_reports() to
  fetch candidates, then delete_artifact() to remove each original after
  inserting the compressed archived_summary.

  Optionally: prune_archived_summaries(cutoff) removes very old archives if
  RAG_ARCHIVE_RETENTION_DAYS is set (default: None = keep forever).
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert

# sa_select alias used by ChartSnapshotRepository (matches plan spec convention)
sa_select = select
from sqlalchemy.ext.asyncio import AsyncSession

from chive_shared.storage.models import (
    AgentArtifact,
    ApiCall,
    ChartSnapshot,
    DataProvenance,
    Run,
    RunDigestRecord,
    RunEvent,
    TradeJournalEntry,
)
from chive_shared.logging import get_logger


def _to_uuid(value: "str | uuid.UUID") -> uuid.UUID:
    """Coerce a string-or-UUID identifier to UUID.

    Used at the boundary of every repo method that takes a run_id, since the
    pipeline still passes raw strings while chive-backend passes UUIDs.
    """
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))

logger = get_logger(__name__)


# ── Run repository (v0.4.0 unified runs) ─────────────────────────────────────

# Terminal statuses (uppercase, matches the v0.4.0 check constraint).
_TERMINAL_STATUSES = ("COMPLETE", "FAILED", "CANCELLED", "STALE")


class RunRepository:
    """Persists and retrieves Run rows (v0.4.0 unified model).

    Replaces the previous RunRepository (RunRecord) + IntentRepository
    (RunIntent) split. The Run table is the single source of truth.

    The `run_id` parameter accepted by every method is a UUID — either as a
    `uuid.UUID` instance or the canonical 36-char string form. Strings are
    coerced via `_to_uuid()` at the boundary, so existing call sites that
    pass `str(uuid)` keep working.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, record: Run) -> None:
        """Insert or merge a Run row.

        The caller is expected to have populated `id`, `status`, and `portfolio`.
        For row-creation in modern code prefer `upsert_run_start()` which
        also handles the daemon-vs-standalone bifurcation.
        """
        self._session.add(record)
        logger.debug("run_saved", run_id=str(record.id), status=record.status)

    async def upsert_run_start(
        self,
        *,
        run_id: "uuid.UUID | str",
        portfolio: str,
        session_label: str,
        trigger_type: str,
        session_type: str,
        started_at: datetime,
        universe: str | None = None,
        custom_tickers: list[str] | None = None,
        universe_size: int = 0,
        portfolio_size: int = 0,
    ) -> Run:
        """Transition (or create) a run into RUNNING state.

        Standalone path (cron / direct CLI / daemon-startup pipeline):
          No row pre-exists. INSERT a new Run with status='RUNNING' and all
          metadata populated.

        Daemon path (V2 cloud-trigger):
          chive-backend already inserted the row at trigger time with
          status='PENDING_START'. UPDATE in place: status → 'RUNNING',
          started_at, trigger_type, session_type populated, etc.

        Returns the Run row in its new RUNNING state. Idempotent — calling
        twice with the same run_id from the standalone path is a safe no-op
        (the second call sees status='RUNNING' and just refreshes
        started_at / metadata).
        """
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(Run).where(Run.id == rid).with_for_update()
        )
        run = result.scalar_one_or_none()

        if run is None:
            # Standalone path — INSERT.
            run = Run(
                id=rid,
                portfolio=portfolio,
                status="RUNNING",
                universe=universe,
                custom_tickers=custom_tickers,
                session_label=session_label,
                requested_at=started_at,
                picked_up_at=started_at,
                trigger_type=trigger_type,
                session_type=session_type,
                started_at=started_at,
                universe_size=universe_size,
                portfolio_size=portfolio_size,
            )
            self._session.add(run)
            logger.debug(
                "run_started_standalone", run_id=str(rid), trigger_type=trigger_type,
            )
        else:
            # Daemon path — UPDATE the existing PENDING_START row.
            run.status = "RUNNING"
            run.started_at = started_at
            run.trigger_type = trigger_type
            run.session_type = session_type
            if run.picked_up_at is None:
                run.picked_up_at = started_at
            if universe and run.universe is None:
                run.universe = universe
            if custom_tickers and not run.custom_tickers:
                run.custom_tickers = custom_tickers
            if universe_size and not run.universe_size:
                run.universe_size = universe_size
            if portfolio_size and not run.portfolio_size:
                run.portfolio_size = portfolio_size
            logger.debug(
                "run_started_daemon", run_id=str(rid), trigger_type=trigger_type,
            )

        return run

    async def update_status(
        self,
        run_id: "uuid.UUID | str",
        status: str | None = None,
        completed_at: datetime | None = None,
        error: str | None = None,
        action_item_count: int = 0,
        total_cost_usd: float | None = None,
    ) -> None:
        """Update run status fields in-place.

        `status` is normalized to uppercase to match the v0.4.0 check constraint
        (the legacy RunRecord accepted lowercase values like 'complete').

        Pass status=None to skip the status column.
        """
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(Run).where(Run.id == rid)
        )
        record = result.scalar_one_or_none()
        if record is None:
            logger.warning("run_update_not_found", run_id=str(rid))
            return

        if status is not None:
            record.status = status.upper()
        if completed_at is not None:
            record.completed_at = completed_at
        if error is not None:
            record.error = error
        if action_item_count:
            record.action_item_count = action_item_count
        if total_cost_usd is not None:
            record.total_cost_usd = total_cost_usd

        logger.debug("run_status_updated", run_id=str(rid), status=record.status)

    async def get(self, run_id: "uuid.UUID | str") -> Run | None:
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(Run).where(Run.id == rid)
        )
        return result.scalar_one_or_none()

    async def mark_orphaned_as_failed(self, cutoff: datetime) -> int:
        """
        Mark any non-terminal runs older than cutoff as FAILED.

        Called at pipeline startup. Cleans up runs left in intermediate states
        by a previous process that was killed (Ctrl+C, OOM, crash).
        Returns number of runs cleaned up.

        Uses started_at when populated, else requested_at — the unified Run
        table mixes both populated-at-trigger-time and populated-at-start-time
        timestamps.
        """
        from sqlalchemy import func
        when_col = func.coalesce(Run.started_at, Run.requested_at)
        result = await self._session.execute(
            select(Run)
            .where(Run.status.not_in(_TERMINAL_STATUSES))
            .where(when_col < cutoff)
        )
        records = list(result.scalars().all())
        now = datetime.now(UTC)
        for record in records:
            stuck_status = record.status  # capture before overwriting
            record.status = "FAILED"
            record.error = "Process interrupted — run orphaned (previous process killed or crashed)"
            record.completed_at = now
            anchor = record.started_at or record.requested_at
            logger.warning(
                "orphaned_run_marked_failed",
                run_id=str(record.id),
                stuck_at=stuck_status,
                started_at=anchor.isoformat() if anchor else None,
            )
        return len(records)

    async def list_recent(self, limit: int = 20) -> list[Run]:
        """Return the most recent real runs, newest first.

        Rows with trigger_type="test" (written by integration tests) are
        excluded so they never pollute the user-facing list-runs output.

        Ordered by COALESCE(started_at, requested_at) so daemon-path runs
        not yet started, and standalone runs, both sort correctly.
        """
        from sqlalchemy import func
        order_col = func.coalesce(Run.started_at, Run.requested_at)
        result = await self._session.execute(
            select(Run)
            .where((Run.trigger_type != "test") | (Run.trigger_type.is_(None)))
            .order_by(order_col.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_last_successful_completed_at(self) -> "datetime | None":
        """Return completed_at of the most recent successful (non-test) run."""
        result = await self._session.execute(
            select(Run.completed_at)
            .where(Run.status == "COMPLETE")
            .where((Run.trigger_type != "test") | (Run.trigger_type.is_(None)))
            .order_by(Run.completed_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    # ── Daemon-only intent transition helpers (formerly IntentRepository) ───
    # These keep the V2 daemon's state-machine code working unchanged.

    async def pick_next_pending(self) -> "Run | None":
        """Return the oldest PENDING_START run, or None."""
        result = await self._session.execute(
            select(Run)
            .where(Run.status == "PENDING_START")
            .order_by(Run.requested_at.asc())
            .limit(1)
        )
        return result.scalars().first()

    async def mark_starting(self, run: Run) -> None:
        run.status = "STARTING"
        run.picked_up_at = datetime.now(UTC)

    async def mark_running(self, run: Run, *, run_id: "uuid.UUID | str | None" = None) -> None:
        """Transition to RUNNING. `run_id` kwarg is accepted for backward
        compatibility with the legacy IntentRepository signature — it's
        redundant now that intent.id == run.id, and is ignored if it matches
        the row's id."""
        if run_id is not None:
            rid = _to_uuid(run_id)
            if rid != run.id:
                logger.warning(
                    "mark_running_runid_mismatch",
                    expected=str(run.id), got=str(rid),
                )
        run.status = "RUNNING"

    async def mark_complete(self, run: Run) -> None:
        run.status = "COMPLETE"
        run.completed_at = datetime.now(UTC)

    async def mark_failed(
        self, run: Run, *, reason: str, error: str | None = None,
    ) -> None:
        run.status = "FAILED"
        run.failure_reason = reason
        run.error = error
        run.completed_at = datetime.now(UTC)

    async def mark_cancelled(self, run: Run) -> None:
        """Graceful cancellation terminal state."""
        run.status = "CANCELLED"
        run.completed_at = datetime.now(UTC)

    async def get_cancel_signal_for(self, run_id: "uuid.UUID | str") -> str | None:
        """Returns 'graceful' (PENDING_STOP), 'force' (PENDING_FORCE_STOP), or None.

        The daemon polls this between stages to check for user-issued cancel.
        """
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(Run.status).where(Run.id == rid)
        )
        status = result.scalar_one_or_none()
        if status == "PENDING_STOP":
            return "graceful"
        if status == "PENDING_FORCE_STOP":
            return "force"
        return None

    async def find_orphans(self, daemon_id: str) -> "list[Run]":
        """Return all non-terminal runs — assumed orphaned at daemon startup.

        Covers two orphan classes that both need cleanup when the daemon
        crashes or is killed:

        - **Executing orphans** (STARTING, RUNNING) — a run was mid-execution
          when the daemon died. Caller should mark FAILED (`daemon_died_mid_run`).
        - **Stopping orphans** (PENDING_STOP, STOPPING, PENDING_FORCE_STOP) —
          the user requested cancellation but the daemon died before completing
          the transition to a terminal state. Caller should mark CANCELLED
          (user's intent was cancellation; honor it).

        Caller inspects `run.status` to route to the correct terminal state.

        For V1 single-daemon-per-DB assumption, all non-terminal runs are
        considered orphans. The `daemon_id` parameter is reserved for future
        multi-daemon support and currently ignored by the query.
        """
        result = await self._session.execute(
            select(Run)
            .where(Run.status.in_([
                "STARTING", "RUNNING",
                "PENDING_STOP", "STOPPING", "PENDING_FORCE_STOP",
            ]))
        )
        return list(result.scalars().all())


# ── Agent artifact repository ─────────────────────────────────────────────────

class AgentArtifactRepository:
    """
    Persists per-agent outputs and supports vector similarity search.

    Two write paths:
      save()          — store the output_json (no embedding yet)
      save_embedding() — attach the embedding vector to an existing artifact

    One read path:
      search_by_similarity() — cosine similarity search within a ticker scope

    Retention:
      prune_old_artifacts() — delete rows older than the given cutoff date.
      Called once per run to enforce the 90-day rolling window.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, artifact: AgentArtifact) -> None:
        """Persist a new artifact row (embedding can be None at this point)."""
        self._session.add(artifact)
        logger.debug(
            "artifact_saved",
            run_id=artifact.run_id,
            ticker=artifact.ticker,
            agent=artifact.agent_name,
        )

    async def save_embedding(
        self, artifact_id: str, embedding: list[float]
    ) -> None:
        """Attach the computed embedding to an already-persisted artifact."""
        from sqlalchemy import update as sa_update
        from chive_shared.storage.models import AgentArtifact as _AA
        import uuid as _uuid

        await self._session.execute(
            sa_update(_AA)
            .where(_AA.id == _uuid.UUID(str(artifact_id)))
            .values(embedding=embedding)
        )
        logger.debug("artifact_embedding_saved", artifact_id=str(artifact_id))

    async def search_by_similarity(
        self,
        query_vector: list[float],
        ticker: str | None = None,
        k: int = 5,
        agent_names: list[str] | None = None,
        sector: str | None = None,
    ) -> list[AgentArtifact]:
        """
        Return up to k artifacts most semantically similar to query_vector.

        Scope filters (at least one required):
          ticker: restrict to a single ticker — the default for agent retrieval.
                  Hard prevents cross-ticker contamination in per-stock prompts.
          sector: restrict to a GICS sector (e.g. "Technology") — used for
                  sector-rotation analysis and advisor-level cross-ticker queries.
          Both:   intersects ticker + sector (redundant in practice, but valid).

        Uses pgvector cosine distance operator (<=>). Lower distance = higher
        similarity. Results ordered closest-first.

        agent_names: optional allowlist to restrict to specific agent types
        (e.g. ["stock_verdict", "scenario_researcher"]).
        """
        if ticker is None and sector is None:
            raise ValueError("search_by_similarity requires at least one of ticker or sector")

        from sqlalchemy import text as sa_text

        # asyncpg has no built-in vector codec — pass as Postgres text representation
        vec_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        params: dict[str, Any] = {"k": k, "vec": vec_str}

        # Build WHERE clauses dynamically.
        # source="pipeline" is always enforced — test artifacts must never
        # be returned to real agents regardless of ticker or sector match.
        where_clauses = ["embedding IS NOT NULL", "source = 'pipeline'"]

        if ticker is not None:
            where_clauses.append("ticker = :ticker")
            params["ticker"] = ticker

        if sector is not None:
            where_clauses.append("sector = :sector")
            params["sector"] = sector

        if agent_names:
            placeholders = ", ".join(f":an_{i}" for i in range(len(agent_names)))
            where_clauses.append(f"agent_name IN ({placeholders})")
            for i, name in enumerate(agent_names):
                params[f"an_{i}"] = name

        where_sql = " AND ".join(where_clauses)

        stmt = sa_text(f"""
            SELECT id
            FROM agent_artifacts
            WHERE {where_sql}
            ORDER BY embedding <=> CAST(:vec AS vector)
            LIMIT :k
        """)

        result = await self._session.execute(stmt, params)
        ids = [row[0] for row in result.fetchall()]

        if not ids:
            return []

        # Fetch full ORM objects for the matched IDs
        artifacts_result = await self._session.execute(
            select(AgentArtifact).where(AgentArtifact.id.in_(ids))
        )
        artifacts = list(artifacts_result.scalars().all())

        # Re-order to match similarity ranking
        id_order = {id_: idx for idx, id_ in enumerate(ids)}
        artifacts.sort(key=lambda a: id_order.get(a.id, 999))
        return artifacts

    async def get_stale_raw_reports(self, cutoff_date: datetime) -> list[AgentArtifact]:
        """
        Return all raw_report artifacts created before cutoff_date.

        These are candidates for archiving by RAGArchiver — the caller is
        responsible for compressing each and calling delete_artifact() after.
        """
        result = await self._session.execute(
            select(AgentArtifact)
            .where(AgentArtifact.memory_type == "raw_report")
            .where(AgentArtifact.created_at < cutoff_date)
        )
        return list(result.scalars().all())

    async def delete_artifact(self, artifact_id) -> None:
        """Delete a single artifact by primary key (used after archiving)."""
        import uuid as _uuid
        pk = artifact_id if isinstance(artifact_id, _uuid.UUID) else _uuid.UUID(str(artifact_id))
        await self._session.execute(
            delete(AgentArtifact).where(AgentArtifact.id == pk)
        )
        logger.debug("artifact_deleted", artifact_id=str(artifact_id))

    async def get_recent_portfolio_snapshots(self, days: int = 90, portfolio_name: str = "") -> list[dict]:
        """
        Return portfolio snapshot dicts from the last `days` days, oldest first.

        Scoped to portfolio_name when provided — prevents snapshots from a
        different portfolio polluting the reconciler's historical view.
        """
        from sqlalchemy import cast
        from sqlalchemy.dialects.postgresql import JSONB
        cutoff = datetime.now(UTC) - timedelta(days=days)
        q = (
            select(AgentArtifact)
            .where(AgentArtifact.agent_name == "portfolio_snapshot")
            .where(AgentArtifact.created_at >= cutoff)
        )
        if portfolio_name:
            q = q.where(
                AgentArtifact.output_json["portfolio_name"].as_string() == portfolio_name
            )
        q = q.order_by(AgentArtifact.created_at)
        result = await self._session.execute(q)
        artifacts = list(result.scalars().all())
        return [a.output_json for a in artifacts if a.output_json]

    async def get_latest_recommendation(
        self,
        portfolio_name: str = "",
    ) -> dict | None:
        """M29.8-B — Return the output_json of the most recent
        FinalRecommendation (agent_name="portfolio_manager") artifact.

        Used by the trade CLI's `--from-latest` flag to auto-populate
        entry context (regime, stop, pyramid plan) from CHIVE's most
        recent recommendation. Returns None when no recommendations
        have been persisted yet.

        Multi-portfolio scoping (two-tier query):
          1. Strict filter on output_json["portfolio_name"] — used for
             runs persisted after the M29.8-B multi-portfolio fix.
          2. Legacy fallback: when (1) returns nothing, query unfiltered
             — picks up pre-fix runs that don't have portfolio_name in
             their output_json. Returns the carried-along `legacy` flag
             so the caller can warn the user about possible cross-
             portfolio bleed (single-portfolio users won't notice).

        Empty `portfolio_name` skips tier 1 and uses tier 2 directly
        (callers that don't care about portfolio scoping).
        """
        # Tier 1: strict portfolio-filtered query.
        if portfolio_name:
            q1 = (
                select(AgentArtifact)
                .where(AgentArtifact.agent_name == "portfolio_manager")
                .where(
                    AgentArtifact.output_json["portfolio_name"].as_string() == portfolio_name
                )
                .order_by(AgentArtifact.created_at.desc())
                .limit(1)
            )
            result = await self._session.execute(q1)
            artifact = result.scalars().first()
            if artifact is not None:
                return {
                    "output_json": artifact.output_json,
                    "created_at": artifact.created_at,
                    "run_id": artifact.run_id,
                    "legacy": False,
                }

        # Tier 2: legacy fallback — no portfolio filter. Picks up pre-fix
        # runs that don't have portfolio_name in their output_json.
        q2 = (
            select(AgentArtifact)
            .where(AgentArtifact.agent_name == "portfolio_manager")
            .order_by(AgentArtifact.created_at.desc())
            .limit(1)
        )
        result = await self._session.execute(q2)
        artifact = result.scalars().first()
        if artifact is None:
            return None
        # Mark this as legacy if either:
        #  - The caller specified a portfolio_name but tier 1 found nothing
        #    (meaning we fell back to unfiltered), OR
        #  - The artifact's output_json doesn't have portfolio_name set
        oj = artifact.output_json or {}
        is_legacy = bool(portfolio_name) or not oj.get("portfolio_name")
        return {
            "output_json": artifact.output_json,
            "created_at": artifact.created_at,
            "run_id": artifact.run_id,
            "legacy": is_legacy,
        }

    async def get_last_ips_review(self, portfolio_name: str = "") -> dict | None:
        """
        Return the most recent IPS review output_json for the given portfolio.

        Scoped to portfolio_name so switching portfolios gets a fresh first review
        rather than seeing another portfolio's last review as its history.
        """
        q = (
            select(AgentArtifact)
            .where(AgentArtifact.agent_name == "ips_review")
        )
        if portfolio_name:
            q = q.where(
                AgentArtifact.output_json["portfolio_name"].as_string() == portfolio_name
            )
        q = q.order_by(AgentArtifact.created_at.desc()).limit(1)
        result = await self._session.execute(q)
        artifact = result.scalars().first()
        return artifact.output_json if artifact else None

    async def ips_review_exists_this_month(self, portfolio_name: str = "") -> bool:
        """
        True if an IPS review was persisted this calendar month for the given portfolio.

        Scoped to portfolio_name so switching portfolios always triggers a fresh
        first review rather than being blocked by another portfolio's monthly review.
        """
        from sqlalchemy import extract, func
        now = datetime.now(UTC)
        q = (
            select(func.count())
            .select_from(AgentArtifact)
            .where(AgentArtifact.agent_name == "ips_review")
            .where(extract("year",  AgentArtifact.created_at) == now.year)
            .where(extract("month", AgentArtifact.created_at) == now.month)
        )
        if portfolio_name:
            q = q.where(
                AgentArtifact.output_json["portfolio_name"].as_string() == portfolio_name
            )
        result = await self._session.execute(q)
        return (result.scalar_one() or 0) > 0

    async def prune_archived_summaries(self, cutoff_date: datetime) -> int:
        """
        Delete archived_summary artifacts created before cutoff_date.

        Only called when RAG_ARCHIVE_RETENTION_DAYS is explicitly set.
        Defaults to never (archived summaries are kept indefinitely).
        Returns number of rows deleted.
        """
        result = await self._session.execute(
            delete(AgentArtifact)
            .where(AgentArtifact.memory_type == "archived_summary")
            .where(AgentArtifact.created_at < cutoff_date)
            .returning(AgentArtifact.id)
        )
        deleted_count = len(result.fetchall())
        if deleted_count:
            logger.info(
                "rag_archived_summaries_pruned",
                deleted=deleted_count,
                cutoff=cutoff_date.isoformat(),
            )
        return deleted_count


# ── Run event repository (AuditLogger write path) ─────────────────────────────

class RunEventRepository:
    """Persists RunEvent rows — the AuditLogger's DB write path."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, event: RunEvent) -> None:
        self._session.add(event)

    async def list_for_run(self, run_id: "uuid.UUID | str") -> list[RunEvent]:
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(RunEvent)
            .where(RunEvent.run_id == rid)
            .order_by(RunEvent.created_at)
        )
        return list(result.scalars().all())


# ── Run digest repository ─────────────────────────────────────────────────────

class RunDigestRepository:
    """Persists and retrieves RunDigestRecord rows."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, digest: RunDigestRecord) -> None:
        self._session.add(digest)
        logger.debug("digest_saved", run_id=str(digest.run_id))

    async def get_by_run_id(self, run_id: "uuid.UUID | str") -> RunDigestRecord | None:
        rid = _to_uuid(run_id)
        result = await self._session.execute(
            select(RunDigestRecord).where(RunDigestRecord.run_id == rid)
        )
        return result.scalar_one_or_none()

    async def get_latest(self) -> RunDigestRecord | None:
        # Join unified runs to exclude test runs — same filter as list_recent().
        result = await self._session.execute(
            select(RunDigestRecord)
            .join(Run, Run.id == RunDigestRecord.run_id)
            .where((Run.trigger_type != "test") | (Run.trigger_type.is_(None)))
            .order_by(RunDigestRecord.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


# ── API call repository ───────────────────────────────────────────────────────

class ApiCallRepository:
    """Persists ApiCall rows — one per external HTTP request."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, call: ApiCall) -> None:
        self._session.add(call)

    async def list_for_run(self, run_id: str) -> list[ApiCall]:
        result = await self._session.execute(
            select(ApiCall)
            .where(ApiCall.run_id == run_id)
            .order_by(ApiCall.created_at)
        )
        return list(result.scalars().all())


# ── Data provenance repository ────────────────────────────────────────────────

class DataProvenanceRepository:
    """Persists DataProvenance rows — news source tracking per (run × ticker)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save_batch(self, records: list[DataProvenance]) -> None:
        for r in records:
            self._session.add(r)

    async def list_for_run(
        self, run_id: str, ticker: str | None = None
    ) -> list[DataProvenance]:
        stmt = (
            select(DataProvenance)
            .where(DataProvenance.run_id == run_id)
            .order_by(DataProvenance.fetched_at)
        )
        if ticker is not None:
            stmt = stmt.where(DataProvenance.ticker == ticker)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ── Trade journal repository (M29.8-C) ───────────────────────────────────────


class TradeJournalRepository:
    """Persists TradeJournalEntry rows — full long-term closed-trade history.

    INSERTs use ON CONFLICT DO NOTHING on the (portfolio_name, ticker,
    closed_date, shares, sold_price) unique constraint so replay-from-JSON
    is idempotent — calling save_entry() repeatedly with the same trade
    fingerprint never produces duplicates.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save_entry(
        self, *,
        portfolio_name: str,
        ticker: str,
        closed_date,
        shares: float,
        sold_price: float,
        cost_basis_avg: float,
        realized_gain: float,
        entry_reason: str | None = None,
        market_regime_at_entry: str | None = None,
        stop_loss_at_entry: float | None = None,
        pyramid_plan_at_entry: dict | None = None,
        exit_reason: str | None = None,
        post_mortem_note: str | None = None,
        holding_days: int | None = None,
        chive_alignment: str | None = None,
    ) -> None:
        """Idempotent insert — duplicate fingerprints are silently skipped."""
        stmt = pg_insert(TradeJournalEntry).values(
            portfolio_name=portfolio_name,
            ticker=ticker.upper(),
            closed_date=closed_date,
            shares=shares,
            sold_price=sold_price,
            cost_basis_avg=cost_basis_avg,
            realized_gain=realized_gain,
            entry_reason=entry_reason,
            market_regime_at_entry=market_regime_at_entry,
            stop_loss_at_entry=stop_loss_at_entry,
            pyramid_plan_at_entry=pyramid_plan_at_entry,
            exit_reason=exit_reason,
            post_mortem_note=post_mortem_note,
            holding_days=holding_days,
            chive_alignment=chive_alignment,
        ).on_conflict_do_nothing(
            constraint="uq_trade_journal_fingerprint",
        )
        await self._session.execute(stmt)

    async def list_for_portfolio(self, portfolio_name: str) -> list[TradeJournalEntry]:
        """Return all trades for a portfolio, newest first."""
        result = await self._session.execute(
            select(TradeJournalEntry)
            .where(TradeJournalEntry.portfolio_name == portfolio_name)
            .order_by(TradeJournalEntry.closed_date.desc())
        )
        return list(result.scalars().all())

    async def exists_fingerprint(
        self, *,
        portfolio_name: str, ticker: str,
        closed_date, shares: float, sold_price: float,
    ) -> bool:
        """Cheap dedup check used by the replay routine."""
        result = await self._session.execute(
            select(TradeJournalEntry.id)
            .where(TradeJournalEntry.portfolio_name == portfolio_name)
            .where(TradeJournalEntry.ticker == ticker.upper())
            .where(TradeJournalEntry.closed_date == closed_date)
            .where(TradeJournalEntry.shares == shares)
            .where(TradeJournalEntry.sold_price == sold_price)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


# ── Daemon Status repository (M33 / V2) ─────────────────────────────────────


class DaemonStatusRepository:
    """Persists DaemonStatus rows — heartbeat signal from the local daemon.

    Single row per daemon_id, upserted every poll interval (~30s). The cloud
    webserver reads to determine online/stale/down state.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_heartbeat(
        self, *,
        daemon_id: str,
        poll_interval_seconds: int = 30,
        hostname: str | None = None,
        pipeline_version: str | None = None,
        current_run_id: str | None = None,
    ) -> None:
        """Insert or update the heartbeat row for this daemon."""
        from chive_shared.storage.models import DaemonStatus
        now = datetime.now(UTC)
        stmt = pg_insert(DaemonStatus).values(
            daemon_id=daemon_id,
            last_heartbeat_at=now,
            poll_interval_seconds=poll_interval_seconds,
            hostname=hostname,
            pipeline_version=pipeline_version,
            current_run_id=current_run_id,
            updated_at=now,
        ).on_conflict_do_update(
            index_elements=["daemon_id"],
            set_={
                "last_heartbeat_at": now,
                "poll_interval_seconds": poll_interval_seconds,
                "hostname": hostname,
                "pipeline_version": pipeline_version,
                "current_run_id": current_run_id,
                "updated_at": now,
            },
        )
        await self._session.execute(stmt)

    async def get_status(self, daemon_id: str) -> "DaemonStatus | None":
        from chive_shared.storage.models import DaemonStatus
        result = await self._session.execute(
            select(DaemonStatus).where(DaemonStatus.daemon_id == daemon_id)
        )
        return result.scalar_one_or_none()


# ── Chart snapshot repository (M36) ──────────────────────────────────────────


class ChartSnapshotRepository:
    """Persists OHLCV bars + indicators + annotations per (run, ticker)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, snapshot: ChartSnapshot) -> None:
        """Insert or replace the snapshot for (run_id, ticker)."""
        stmt = (
            postgresql.insert(ChartSnapshot)
            .values(
                run_id=snapshot.run_id,
                ticker=snapshot.ticker,
                bars=snapshot.bars,
                indicators=snapshot.indicators,
                annotations=snapshot.annotations,
            )
            .on_conflict_do_update(
                index_elements=["run_id", "ticker"],
                set_=dict(
                    bars=snapshot.bars,
                    indicators=snapshot.indicators,
                    annotations=snapshot.annotations,
                ),
            )
        )
        await self.session.execute(stmt)

    async def get(self, run_id: uuid.UUID, ticker: str) -> ChartSnapshot | None:
        stmt = sa_select(ChartSnapshot).where(
            ChartSnapshot.run_id == run_id,
            ChartSnapshot.ticker == ticker,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_for_run(self, run_id: uuid.UUID) -> list[ChartSnapshot]:
        stmt = sa_select(ChartSnapshot).where(ChartSnapshot.run_id == run_id)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())


# ── Backwards-compatibility aliases (v0.4.0 unified-runs refactor) ───────────
# IntentRepository was merged into RunRepository (the daemon-state-machine
# helpers — pick_next_pending, mark_starting/running/complete/failed,
# get_cancel_signal_for, find_orphans — are now methods on RunRepository).
# This alias lets the V2 daemon code keep its `IntentRepository` import
# working until M35 step 3 updates it.
IntentRepository = RunRepository
RunIntentRepository = RunRepository
