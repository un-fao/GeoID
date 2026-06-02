"""Real-DB integration coverage for the #735 liveness helpers — #741 item 1.

The #735 unit tests pin SQL shape via source-inspection and mock the engine.
The load-bearing remaining unknowns are:

1. The new ``runner_ref TEXT`` column applies cleanly when the global tasks
   table is materialized (no migration shim — fresh schema or DB renewal only).
2. ``set_runner_ref`` actually writes that column without touching status.
3. ``persist_outputs`` actually writes ``outputs``/``progress`` without
   flipping the status — the two-phase safety the reconciler-driven
   TERMINAL_SUCCEEDED recovery depends on.
4. ``select_lapsed_gcp_tasks`` actually picks up the rows it should and
   leaves the rest alone, against a live partitioned tasks table — under the
   true Postgres semantics for ``status``, ``locked_until`` and the
   ``owner_id LIKE 'gcp_cloud_run_%'`` predicate.
5. ``heartbeat_task_if_active`` actually returns ``True`` when it extends an
   ACTIVE row and ``False`` when the row is no longer ACTIVE — the reaper-race
   signal the reconciler observability layer depends on (#741 item 3).

Failure here means the reconciler is silently broken at deploy time on any
non-renewed environment.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]


async def _birth_claim_gcp_row(
    engine,
    *,
    schema_name: str,
    owner_id: str,
    locked_until: datetime,
    runner_ref: str | None = None,
    task_type: str | None = None,
) -> uuid.UUID:
    """Create a tasks row born-claimed ACTIVE for a gcp_cloud_run owner.

    Uses the production ``create_task`` born-claimed path (the path
    ``GcpJobRunner`` itself uses on the REST flow) so the row reflects what
    the reconciler will encounter in production. ``runner_ref`` is stamped
    via the dedicated helper afterwards to mirror the actual spawn sequence.
    """
    task = await tasks_module.create_task(
        engine,
        TaskCreate(
            task_type=task_type or f"ingest_test_{generate_test_id(6)}",
            caller_id="liveness-roundtrip-test",
        ),
        schema=schema_name,
        initial_status="ACTIVE",
        owner_id=owner_id,
        locked_until=locked_until,
    )
    assert task is not None, "born-claimed row must be created"
    if runner_ref is not None:
        await tasks_module.set_runner_ref(engine, task.task_id, runner_ref)
    return task.task_id


@pytest_asyncio.fixture(loop_scope="function")
async def _gcp_row_factory(task_app_state):
    """Yields ``(create, schema, engine)`` and force-deletes everything it created.

    On a freshly-renewed DB the ``runner_ref TEXT`` column comes from the
    ``CREATE TABLE`` DDL applied at bootstrap. On a stale local / review DB
    (see the open risk carried from #735) the column is missing until the DB
    is renewed; an idempotent ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` here
    keeps these tests meaningful without shipping a migration shim — the
    directive 'no migration code on draft schema' still holds in production.
    The DDL contract itself is pinned by the source-inspection unit test
    ``test_ddl_has_runner_ref_column`` so a regression in the DDL string
    fails RED before reaching here.
    """
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f'ALTER TABLE "{task_schema}".tasks '
                "ADD COLUMN IF NOT EXISTS runner_ref TEXT"
            )
        )

    schema_name = f"test_liveness_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create(**kwargs) -> uuid.UUID:
        kwargs.setdefault("schema_name", schema_name)
        task_id = await _birth_claim_gcp_row(engine, **kwargs)
        created.append(task_id)
        return task_id

    yield _create, schema_name, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


# --- runner_ref column applies cleanly ------------------------------------


async def test_runner_ref_column_present_after_factory_setup(_gcp_row_factory):
    """The materialized global tasks table must carry ``runner_ref TEXT`` —
    if the DDL didn't apply (e.g. stale environment, missing renewal) every
    probe degrades to UNKNOWN and #735 is effectively un-shipped. The factory
    fixture's ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` covers the stale-DB
    case for the local test runner; the live behaviour we pin here is that
    the column is then visible at the SQL level with the expected type."""
    _, _, engine = _gcp_row_factory
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (await conn.execute(
            text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = 'tasks' "
                "AND column_name = 'runner_ref'"
            ),
            {"s": task_schema},
        )).fetchone()
    assert row is not None, "runner_ref column missing from tasks table"
    assert row[0].lower() == "text"


# --- set_runner_ref round-trip --------------------------------------------


async def test_set_runner_ref_writes_column_without_status_churn(_gcp_row_factory):
    """The helper stamps the handle on the row and leaves status untouched."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    ref = f"projects/p/locations/r/jobs/j/executions/exec-{generate_test_id(6)}"
    await tasks_module.set_runner_ref(engine, task_id, ref)

    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (await conn.execute(
            text(
                f'SELECT runner_ref, status FROM "{task_schema}".tasks '
                "WHERE task_id = :tid"
            ),
            {"tid": task_id},
        )).fetchone()
    assert row is not None
    assert row[0] == ref
    assert row[1] == "ACTIVE"  # status unchanged — set_runner_ref is metadata-only


# --- persist_outputs round-trip -------------------------------------------


async def test_persist_outputs_writes_outputs_without_flipping_status(_gcp_row_factory):
    """The two-phase safety the reconciler relies on: ``outputs`` are on the
    row *before* the terminal status flip. A reconciler-driven TERMINAL_SUCCEEDED
    recovery would otherwise pick up an empty result."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    payload = {"result": "ok", "rows": 42}
    await tasks_module.persist_outputs(engine, task_id, payload)

    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (await conn.execute(
            text(
                f'SELECT outputs, progress, status FROM "{task_schema}".tasks '
                "WHERE task_id = :tid"
            ),
            {"tid": task_id},
        )).fetchone()
    assert row is not None
    # outputs serializes through CustomJSONEncoder → JSONB column round-trips
    # back as the original dict.
    assert row[0] == payload
    assert row[1] == 100  # progress
    assert row[2] == "ACTIVE"  # status STILL ACTIVE — complete_task owns the flip


# --- select_lapsed_gcp_tasks picks the right rows -------------------------


async def test_select_lapsed_gcp_tasks_picks_lapsed_gcp_active_rows(_gcp_row_factory):
    """A lapsed-lease ACTIVE row with a ``gcp_cloud_run_*`` owner is exactly
    what the reconciler must see. ``select_lapsed_gcp_tasks`` must surface it
    with the full column set the verdict actions consume."""
    create, schema_name, engine = _gcp_row_factory
    owner_id = f"gcp_cloud_run_{generate_test_id(10)}"
    runner_ref = f"projects/p/locations/r/jobs/j/executions/{generate_test_id(8)}"
    task_id = await create(
        owner_id=owner_id,
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
        runner_ref=runner_ref,
    )

    rows = await tasks_module.select_lapsed_gcp_tasks(engine)
    picked = [r for r in rows if r["task_id"] == task_id]
    assert picked, "lapsed ACTIVE gcp_cloud_run_* row must be selected"
    row = picked[0]
    assert row["owner_id"] == owner_id
    assert row["runner_ref"] == runner_ref
    # The verdict actions need these columns:
    for col in (
        "schema_name", "task_type", "started_at", "outputs", "retry_count",
        "scope", "caller_id", "inputs", "collection_id",
    ):
        assert col in row, f"select_lapsed_gcp_tasks must surface {col!r}"


async def test_select_lapsed_gcp_tasks_skips_unexpired_lease(_gcp_row_factory):
    """A still-active lease (``locked_until > NOW()``) must NOT be selected —
    reconciling it would steal the row from a healthy heartbeater."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) + timedelta(seconds=300),
    )

    rows = await tasks_module.select_lapsed_gcp_tasks(engine)
    assert task_id not in [r["task_id"] for r in rows]


async def test_select_lapsed_gcp_tasks_skips_non_gcp_owners(_gcp_row_factory):
    """A lapsed row owned by an in-process runner (``BackgroundRunner`` etc.)
    is NOT a gcp row — the pg_cron reaper handles it, the reconciler does
    not. The ``owner_id LIKE 'gcp_cloud_run_%'`` predicate enforces this."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"dispatcher-pod-{generate_test_id(5)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    rows = await tasks_module.select_lapsed_gcp_tasks(engine)
    assert task_id not in [r["task_id"] for r in rows]


# --- heartbeat_task_if_active ---------------------------------------------


async def test_heartbeat_task_if_active_returns_true_for_active_row(_gcp_row_factory):
    """When the row is ACTIVE the helper extends the lease and returns True —
    the reconciler logs the standard ALIVE message."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    extended = await tasks_module.heartbeat_task_if_active(
        engine, task_id, timedelta(seconds=300)
    )
    assert extended is True

    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        new_lu = (await conn.execute(
            text(f'SELECT locked_until FROM "{task_schema}".tasks WHERE task_id = :tid'),
            {"tid": task_id},
        )).scalar_one()
    # locked_until is now in the future — the reaper's next pass skips this row.
    assert new_lu > datetime.now(timezone.utc)


async def test_heartbeat_task_if_active_returns_false_when_no_longer_active(
    _gcp_row_factory,
):
    """When the row has been flipped out of ACTIVE (the reaper-race scenario:
    pg_cron reset it to PENDING between the reconciler's SELECT and its
    UPDATE) the helper matches 0 rows and returns False. That is exactly the
    signal the reconciler turns into the WARNING that surfaces the race."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    # Simulate the reaper having reclaimed the row to PENDING.
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        await conn.execute(
            text(f'UPDATE "{task_schema}".tasks SET status = \'PENDING\' WHERE task_id = :tid'),
            {"tid": task_id},
        )
        await conn.commit()

    extended = await tasks_module.heartbeat_task_if_active(
        engine, task_id, timedelta(seconds=300)
    )
    assert extended is False


# --- fail_task / complete_task owner_id race guard (#750 / #757) -----------
#
# #750 added an opt-in keyword-only ``owner_id`` guard to ``fail_task`` and
# ``complete_task`` so the liveness reconciler can only act on the exact
# execution attempt it probed. The unit tests pin the SQL *shape*; these pin
# the live Postgres *behaviour* — that the guard genuinely filters the UPDATE
# and that a mismatch leaves the row byte-for-byte untouched (the correctness
# property: the reconciler must never clobber a reaper-reclaimed,
# dispatcher-re-dispatched fresh attempt).


async def _read_row(engine, task_id):
    """Return ``(status, owner_id, retry_count)`` for a global-tasks row."""
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        return (await conn.execute(
            text(
                f'SELECT status, owner_id, retry_count '
                f'FROM "{task_schema}".tasks WHERE task_id = :tid'
            ),
            {"tid": task_id},
        )).one()


async def test_fail_task_owner_guard_match_transitions_row(_gcp_row_factory):
    """``fail_task`` with the matching ``owner_id`` updates the row and
    returns ``True`` — the reconciler's DEAD/TERMINAL_FAILED happy path."""
    create, schema_name, engine = _gcp_row_factory
    owner_id = f"gcp_cloud_run_{generate_test_id(10)}"
    task_id = await create(
        owner_id=owner_id,
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    acted = await tasks_module.fail_task(
        engine, task_id, datetime.now(timezone.utc),
        "liveness: execution gone", retry=True, owner_id=owner_id,
    )
    assert acted is True

    status, _, retry_count = await _read_row(engine, task_id)
    # retry=True under the default cap → requeued PENDING, retry_count bumped.
    assert status == "PENDING"
    assert retry_count == 1


async def test_fail_task_owner_guard_mismatch_leaves_row_untouched(_gcp_row_factory):
    """The correctness property: when the probed ``owner_id`` no longer owns
    the row (pg_cron reaper reclaimed it, dispatcher re-dispatched a fresh
    attempt under a new owner), ``fail_task`` matches 0 rows, returns
    ``False``, and the row is left exactly as it was — the fresh attempt is
    not clobbered."""
    create, schema_name, engine = _gcp_row_factory
    owner_id = f"gcp_cloud_run_{generate_test_id(10)}"
    task_id = await create(
        owner_id=owner_id,
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    acted = await tasks_module.fail_task(
        engine, task_id, datetime.now(timezone.utc),
        "liveness: stale probe", retry=True,
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",  # a different attempt
    )
    assert acted is False

    status, row_owner, retry_count = await _read_row(engine, task_id)
    assert status == "ACTIVE"
    assert row_owner == owner_id
    assert retry_count == 0


async def test_complete_task_owner_guard_match_transitions_row(_gcp_row_factory):
    """``complete_task`` with the matching ``owner_id`` flips the row to
    COMPLETED and returns ``True`` — the reconciler's TERMINAL_SUCCEEDED
    recovery path."""
    create, schema_name, engine = _gcp_row_factory
    owner_id = f"gcp_cloud_run_{generate_test_id(10)}"
    task_id = await create(
        owner_id=owner_id,
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    acted = await tasks_module.complete_task(
        engine, task_id, datetime.now(timezone.utc),
        outputs={"reconciled": True}, owner_id=owner_id,
    )
    assert acted is True

    status, row_owner, _ = await _read_row(engine, task_id)
    assert status == "COMPLETED"
    assert row_owner is None  # complete_task clears the lease owner


async def test_complete_task_owner_guard_mismatch_leaves_row_untouched(_gcp_row_factory):
    """Mismatched ``owner_id`` → ``complete_task`` matches 0 rows, returns
    ``False``, row untouched — the reconciler must not complete a fresh
    attempt out from under Cloud Run."""
    create, schema_name, engine = _gcp_row_factory
    owner_id = f"gcp_cloud_run_{generate_test_id(10)}"
    task_id = await create(
        owner_id=owner_id,
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    acted = await tasks_module.complete_task(
        engine, task_id, datetime.now(timezone.utc),
        outputs={"reconciled": True},
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",  # a different attempt
    )
    assert acted is False

    status, row_owner, _ = await _read_row(engine, task_id)
    assert status == "ACTIVE"
    assert row_owner == owner_id


async def test_terminal_helpers_without_owner_id_stay_unconditional(_gcp_row_factory):
    """Back-compat: with no ``owner_id`` passed, ``complete_task`` is
    unconditional exactly as before #750 — every existing (non-reconciler)
    caller keeps today's behaviour and still gets ``True``."""
    create, schema_name, engine = _gcp_row_factory
    task_id = await create(
        owner_id=f"gcp_cloud_run_{generate_test_id(10)}",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    acted = await tasks_module.complete_task(
        engine, task_id, datetime.now(timezone.utc),
    )
    assert acted is True

    status, _, _ = await _read_row(engine, task_id)
    assert status == "COMPLETED"
