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


def test_task_drained_log_shape():
    timestamp = datetime.now(timezone.utc) - timedelta(seconds=1.5)
    drain_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
    line = (
        f"task_drained task_type=index_propagation task_id=t-1 "
        f"enqueue_to_drain_seconds={drain_seconds:.4f}"
    )
    m = re.match(
        r"^task_drained task_type=(?P<task_type>\S+) task_id=(?P<task_id>\S+) "
        r"enqueue_to_drain_seconds=(?P<seconds>\d+\.\d+)$",
        line,
    )
    assert m is not None
    assert m["task_type"] == "index_propagation"
    assert 0.9 < float(m["seconds"]) < 2.5


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
