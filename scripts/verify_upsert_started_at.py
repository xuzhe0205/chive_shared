"""Manual verification of M36 Task 4 — upsert_run_start populates started_at on both branches.

Run with:
    cd ~/Desktop/projects/chive_shared
    uv run python scripts/verify_upsert_started_at.py
"""

import asyncio
import uuid
from datetime import datetime, timezone, timedelta

from chive_shared.storage.db import get_session
from chive_shared.storage.models import Run
from chive_shared.storage.repository import RunRepository


async def main() -> None:
    test_run_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    # ── BRANCH 1: INSERT (standalone path — no pre-existing row) ────────
    async with get_session() as session:
        repo = RunRepository(session)
        await repo.upsert_run_start(
            run_id=test_run_id,
            portfolio="m36-task4-verify",
            session_label="verify",
            trigger_type="manual",
            session_type="manual",
            started_at=now,
            universe="custom",
            custom_tickers=None,
            universe_size=0,
            portfolio_size=0,
        )
        await session.commit()
    async with get_session() as session:
        repo = RunRepository(session)
        row = await repo.get(test_run_id)
    assert row is not None, "INSERT branch failed — row not found"
    assert row.started_at is not None, f"INSERT branch failed — started_at is NULL"
    assert row.status == "RUNNING", f"expected RUNNING, got {row.status!r}"
    print(f"✓ INSERT branch: started_at = {row.started_at.isoformat()}")

    # ── BRANCH 2: UPDATE (daemon path — pre-existing PENDING_START row) ─
    daemon_run_id = uuid.uuid4()
    async with get_session() as session:
        # Simulate chive-backend's PENDING_START insert (no started_at)
        run = Run(
            id=daemon_run_id,
            portfolio="m36-task4-verify",
            status="PENDING_START",
            session_label="verify-daemon",
            requested_at=now,
        )
        session.add(run)
        await session.commit()

    later = now + timedelta(seconds=30)
    async with get_session() as session:
        repo = RunRepository(session)
        await repo.upsert_run_start(
            run_id=daemon_run_id,
            portfolio="m36-task4-verify",
            session_label="verify-daemon",
            trigger_type="manual",
            session_type="manual",
            started_at=later,
            universe=None,
            custom_tickers=None,
            universe_size=0,
            portfolio_size=0,
        )
        await session.commit()
    async with get_session() as session:
        repo = RunRepository(session)
        row = await repo.get(daemon_run_id)
    assert row is not None, "UPDATE branch failed — row missing"
    assert row.started_at is not None, "UPDATE branch failed — started_at still NULL"
    assert row.status == "RUNNING", f"UPDATE branch failed — status {row.status!r}"
    print(f"✓ UPDATE branch: started_at = {row.started_at.isoformat()}")

    # ── Cleanup ─────────────────────────────────────────────────────────
    async with get_session() as session:
        from sqlalchemy import delete
        await session.execute(delete(Run).where(Run.id.in_([test_run_id, daemon_run_id])))
        await session.commit()
    print("✓ cleanup done")
    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
