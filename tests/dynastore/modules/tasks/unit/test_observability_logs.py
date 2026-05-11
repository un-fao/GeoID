"""#504 — structured log lines for index dispatch observability."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from dynastore.models.protocols.indexer import IndexContext, IndexOp
from dynastore.modules.storage.index_dispatcher import TaskTableOutboxWriter


@pytest.mark.asyncio
async def test_outbox_enqueue_emits_chunk_emitted_log(caplog):
    writer = TaskTableOutboxWriter(task_schema_resolver=lambda: "tasks")
    conn = MagicMock()
    ctx = IndexContext(
        catalog="catA", collection="colB", correlation_id="cid",
        pg_conn=conn,
    )
    op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="e1", payload={},
    )

    async def _noop_exec(*_a, **_kw):
        return None

    writer._exec_insert = _noop_exec  # type: ignore[assignment]

    with caplog.at_level(logging.INFO, logger="dynastore.modules.storage.index_dispatcher"):
        await writer.enqueue(indexer_id="ix1", ctx=ctx, op=op)

    matches = [r for r in caplog.records if r.getMessage().startswith("index_chunk_emitted ")]
    assert len(matches) == 1
    msg = matches[0].getMessage()
    assert "indexer=ix1" in msg
    assert "source=legacy" in msg
    assert "op_type=upsert" in msg
    assert "catalog=catA" in msg
    assert "chunk_size=1" in msg


def test_task_drained_success_emission(caplog):
    from dynastore.modules.tasks.dispatcher import _log_task_terminal

    ts = datetime.now(timezone.utc) - timedelta(seconds=0.5)
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher"):
        _log_task_terminal(
            "index_propagation", "t-1", ts, outcome="success", error=None,
        )
    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("task_drained ") for m in msgs)
    m = next(m for m in msgs if m.startswith("task_drained "))
    assert "task_type=index_propagation" in m
    assert "outcome=success" in m
    assert re.search(r"enqueue_to_drain_seconds=\d+\.\d+", m)


def test_task_failed_emission_per_outcome(caplog):
    """All three failure paths emit `task_failed` with distinct outcomes."""
    from dynastore.modules.tasks.dispatcher import _log_task_terminal

    ts = datetime.now(timezone.utc) - timedelta(seconds=0.1)
    outcomes = ("cancelled", "permanent_failure", "transient_failure")
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher"):
        for outcome in outcomes:
            _log_task_terminal(
                "index_propagation", "t-X", ts, outcome=outcome, error="boom",
            )

    failed_msgs = [
        r.getMessage() for r in caplog.records
        if r.getMessage().startswith("task_failed ")
    ]
    assert len(failed_msgs) == 3
    for outcome, msg in zip(outcomes, failed_msgs):
        assert f"outcome={outcome}" in msg
        assert "error='boom'" in msg
        assert re.search(r"enqueue_to_drain_seconds=\d+\.\d+", msg)


def test_task_claim_rejected_log_shape():
    line = "task_claim_rejected task_type=index_propagation capability=ix1 task_id=t-1 — can_claim returned False on this worker"
    m = re.match(
        r"^task_claim_rejected task_type=(?P<task_type>\S+) "
        r"capability=(?P<capability>\S+) task_id=(?P<task_id>\S+) — ",
        line,
    )
    assert m is not None
    assert m["task_type"] == "index_propagation"
    assert m["capability"] == "ix1"
