"""Unit tests for the ``create_task(initial_status='ACTIVE', owner_id=...,
locked_until=...)`` *born-claimed* INSERT path and the ``claim_for_dispatch``
helper introduced to close the REST↔dispatcher race that previously spawned
two Cloud Run Job executions per ingestion request.

Race recap (closed by these primitives):

1. REST handler called ``create_task(initial_status='PENDING')``; row INSERT
   committed.
2. Trigger ``on_task_insert`` (``WHEN NEW.status = 'PENDING'``) fired
   ``pg_notify('new_task_queued', task_type)``.
3. Every dispatcher pod LISTENing on the channel woke and called
   ``claim_batch`` (``FOR UPDATE SKIP LOCKED``) — one claimed the row →
   ACTIVE under its ``owner_id`` and dispatched to ``GcpJobRunner`` →
   Cloud Run RunJob #2.
4. Meanwhile the REST handler continued: ``update_task(status='ACTIVE')``
   (silent no-op), then Cloud Run RunJob #1.

Result: two Cloud Run executions for one task UUID, started microseconds
apart.

The fix INSERTs the row directly as ACTIVE with ``owner_id`` and
``locked_until`` set in *one statement*. The trigger condition is false →
no ``pg_notify`` → no claim_batch wake-up → exactly one Cloud Run spawn.
``claim_for_dispatch`` is the dispatcher-path companion (belt-and-
suspenders against any future regression that re-opens the producer-side
race).
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Source-level invariants — the trigger only fires for PENDING
# ---------------------------------------------------------------------------


def test_insert_trigger_only_fires_for_pending():
    """The ``on_task_insert`` trigger MUST guard on ``NEW.status = 'PENDING'``
    so a born-claimed (ACTIVE) row does not wake every dispatcher pod and
    invite a duplicate-spawn race.

    Asserted by source inspection of the DDL constant — drift here would
    silently re-open the bug.
    """
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_INSERT_TRIGGER_DDL

    normalised = " ".join(GLOBAL_TASKS_INSERT_TRIGGER_DDL.split()).upper()
    assert "WHEN (NEW.STATUS = 'PENDING')" in normalised


def test_create_task_signature_accepts_owner_and_lease():
    """``create_task`` must expose ``owner_id`` and ``locked_until`` as
    keyword-only so callers that wish to INSERT a row already-claimed (the
    REST path of GcpJobRunner) can do so atomically — without a follow-up
    ``update_task(ACTIVE)`` that races the dispatcher's ``claim_batch``.
    """
    from dynastore.modules.tasks.tasks_module import create_task

    sig = inspect.signature(create_task)
    assert "owner_id" in sig.parameters
    assert "locked_until" in sig.parameters
    # Must be keyword-only so positional callers don't accidentally invert
    # arguments and INSERT spurious owner data.
    assert sig.parameters["owner_id"].kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["locked_until"].kind == inspect.Parameter.KEYWORD_ONLY
    # Defaults preserve current behaviour for every other caller.
    assert sig.parameters["owner_id"].default is None
    assert sig.parameters["locked_until"].default is None


def test_create_task_default_path_emits_pending_insert_only():
    """Default invocation must NOT include owner_id / locked_until columns
    in the INSERT; backwards compat for the 30+ existing call sites.
    Verified by source inspection — the SQL builder appends columns
    conditionally, so the default code path stays at the original column
    list."""
    src = inspect.getsource(_get_create_task())
    # Conditional include, not unconditional.
    assert 'cols.append("owner_id")' in src
    assert 'cols.append("locked_until")' in src
    # The ACTIVE-only timing fields must also be conditional so default
    # PENDING inserts do not stamp started_at / last_heartbeat_at out of band.
    assert 'if initial_status == "ACTIVE":' in src
    assert "started_at, last_heartbeat_at" in src


def test_create_task_active_branch_stamps_timing_fields():
    """When ``initial_status='ACTIVE'`` the row MUST also be stamped with
    ``started_at = NOW()`` and ``last_heartbeat_at = NOW()`` so the row
    looks identical to one ``claim_batch`` would have produced. Otherwise
    the reaper would consider the row instantly stale (NULL
    last_heartbeat_at vs locked_until comparison).
    """
    src = inspect.getsource(_get_create_task())
    assert "started_at, last_heartbeat_at" in src
    assert ", NOW(), NOW()" in src


# ---------------------------------------------------------------------------
# claim_for_dispatch — conditional claim for dispatcher path
# ---------------------------------------------------------------------------


def test_claim_for_dispatch_exists_and_is_async():
    """The dispatcher-path defense MUST be a single helper so the SQL has
    one home and can be audited / tested in isolation."""
    from dynastore.modules.tasks.tasks_module import claim_for_dispatch

    assert inspect.iscoroutinefunction(claim_for_dispatch)


def test_claim_for_dispatch_sql_is_idempotent_against_self_and_unowned():
    """The SQL must succeed when the row is unowned OR already owned by
    this owner_id (idempotent re-issue) OR owned by a peer of the same
    runner family (matched by ``expected_owner_prefix LIKE``). It MUST
    refuse when a foreign worker holds the row — that is the regression
    guard against the duplicate-spawn race.
    """
    src = inspect.getsource(_get_claim_for_dispatch())
    assert "owner_id IS NULL" in src
    assert "owner_id LIKE :expected_owner_prefix" in src
    assert "owner_id = :owner_id" in src
    # Must restrict to ACTIVE — claiming a terminal row would clobber state.
    assert "status = 'ACTIVE'" in src
    # Must RETURN the matched task_id so the caller can detect a lost
    # claim (RETURNING empty) and skip side-effects.
    assert "RETURNING task_id" in src


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_create_task():
    from dynastore.modules.tasks.tasks_module import create_task
    return create_task


def _get_claim_for_dispatch():
    from dynastore.modules.tasks.tasks_module import claim_for_dispatch
    return claim_for_dispatch


# ---------------------------------------------------------------------------
# Pydantic guard — Optional[datetime] keyword wires through cleanly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ow,lu", [
    (None, None),
    ("gcp_cloud_run_xyz", None),
    (None, "2026-04-27T12:00:00Z"),
    ("gcp_cloud_run_xyz", "2026-04-27T12:00:00Z"),
])
def test_create_task_kwargs_combinations_accepted(ow, lu):
    """Smoke test for argument shapes — neither raises on construction,
    both nullable, and the new kwargs do not collide with existing params.
    """
    from datetime import datetime
    from dynastore.modules.tasks.tasks_module import create_task

    sig = inspect.signature(create_task)
    bound = sig.bind_partial(
        engine=None,
        task_data=None,
        schema="s_test",
        initial_status="ACTIVE" if ow or lu else "PENDING",
        owner_id=ow,
        locked_until=datetime.fromisoformat(lu.replace("Z", "+00:00")) if lu else None,
    )
    bound.apply_defaults()
    assert bound.arguments["schema"] == "s_test"
