"""Unit tests for the dedup_key plumbing through ExecutionEngine + runners.

Locks the contract introduced in PR following #420:

- ``RunnerContext.dedup_key`` propagates from ``ExecutionEngine.execute(...)``
  to ``TaskCreate(...)`` inside the runner.
- When ``create_task`` returns ``None`` (DB-level dedup hit on
  ``(schema_name, dedup_key)`` partial unique index) AND ``dedup_key`` was
  set, the runner returns ``None`` instead of raising.
- ``ExecutionEngine.execute`` short-circuits the runner-fallback loop on
  None-with-dedup_key — does NOT try the next runner (which would insert a
  new row without dedup awareness).
- Without ``dedup_key``, the legacy "all runners returned None" path still
  raises ``RuntimeError`` (no behavioural change for existing callers).
"""

from __future__ import annotations

import uuid as _uuid
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlalchemy.engine import Engine as _SAEngine

from dynastore.modules.tasks.models import RunnerContext, TaskExecutionMode
from dynastore.modules.tasks.execution import ExecutionEngine


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


def _make_context(dedup_key: Optional[str] = None) -> RunnerContext:
    return RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="gcp_event:OBJECT_FINALIZE",
        inputs={"catalog_id": "c", "collection_id": "col"},
        db_schema="tasks",
        extra_context={},
        dedup_key=dedup_key,
    )


# ---------------------------------------------------------------------------
# RunnerContext propagation
# ---------------------------------------------------------------------------


def test_runner_context_dedup_key_default_is_none():
    """Default behaviour unchanged for callers that don't opt in."""
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="x",
        caller_id="sys",
        inputs={},
        extra_context={},
    )
    assert ctx.dedup_key is None


def test_runner_context_dedup_key_round_trips():
    ctx = _make_context(dedup_key="ingestion:c:col:a:42")
    assert ctx.dedup_key == "ingestion:c:col:a:42"


# ---------------------------------------------------------------------------
# BackgroundRunner — dedup hit returns None instead of raising
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_background_runner_dedup_hit_returns_none():
    """Direct-path BackgroundRunner with dedup_key set: ``create_task``
    returns ``None`` (existing in-flight row), runner MUST NOT raise and
    MUST return ``None`` so the engine can short-circuit."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()
    ctx = _make_context(dedup_key="ingestion:c:col:a:42")

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock(return_value=None)  # dedup hit
    fake_tasks_mgr.update_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"ok": True})

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
    ):
        result = await runner.run(ctx)

    assert result is None
    # The TaskCreate call MUST have carried the dedup_key.
    kwargs = fake_tasks_mgr.create_task.await_args.kwargs
    task_create = fake_tasks_mgr.create_task.await_args.args[1]
    assert task_create.dedup_key == "ingestion:c:col:a:42"
    # Background executor must NOT have been scheduled on a dedup hit.
    fake_task_instance.run.assert_not_called()


# ---------------------------------------------------------------------------
# ExecutionEngine.execute — short-circuit on None-with-dedup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_short_circuits_on_dedup_hit():
    """When dedup_key is set and a runner returns None, the engine MUST NOT
    iterate to the next runner — that would insert a new row without
    dedup awareness, defeating the whole point of the partial unique
    index."""
    engine = ExecutionEngine()

    runner_a = MagicMock()
    runner_a.run = AsyncMock(return_value=None)  # dedup hit
    runner_a.__class__.__name__ = "RunnerA"
    runner_b = MagicMock()
    runner_b.run = AsyncMock(return_value="should-not-be-called")
    runner_b.__class__.__name__ = "RunnerB"

    with patch.object(
        engine, "get_runners_for", return_value=[runner_a, runner_b]
    ):
        result = await engine.execute(
            task_type="ingestion",
            inputs={"x": 1},
            engine=_fake_engine(),
            mode=TaskExecutionMode.ASYNCHRONOUS,
            dedup_key="ingestion:c:col:a:42",
        )

    assert result is None
    runner_a.run.assert_awaited_once()
    runner_b.run.assert_not_called()


@pytest.mark.asyncio
async def test_execute_without_dedup_key_still_raises_when_all_none():
    """Backwards-compatibility: without dedup_key, all runners returning
    None remains a RuntimeError (the legacy contract)."""
    engine = ExecutionEngine()

    runner_a = MagicMock()
    runner_a.run = AsyncMock(return_value=None)
    runner_a.__class__.__name__ = "RunnerA"

    with patch.object(engine, "get_runners_for", return_value=[runner_a]):
        with pytest.raises(RuntimeError, match="All runners"):
            await engine.execute(
                task_type="ingestion",
                inputs={},
                engine=_fake_engine(),
                mode=TaskExecutionMode.ASYNCHRONOUS,
            )


@pytest.mark.asyncio
async def test_execute_propagates_dedup_key_into_runner_context():
    """The dedup_key kwarg on execute() must end up on RunnerContext so
    runners can forward it to TaskCreate."""
    engine = ExecutionEngine()

    captured: Dict[str, Any] = {}

    async def _capture(ctx: RunnerContext):
        captured["dedup_key"] = ctx.dedup_key
        return "ok"

    runner = MagicMock()
    runner.run = _capture
    runner.__class__.__name__ = "CaptureRunner"

    with patch.object(engine, "get_runners_for", return_value=[runner]):
        await engine.execute(
            task_type="ingestion",
            inputs={},
            engine=_fake_engine(),
            mode=TaskExecutionMode.ASYNCHRONOUS,
            dedup_key="ingestion:c:col:a:42",
        )

    assert captured["dedup_key"] == "ingestion:c:col:a:42"
