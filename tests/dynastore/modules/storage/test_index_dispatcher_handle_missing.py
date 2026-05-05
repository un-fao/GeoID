#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Coverage for ``IndexDispatcher._handle_missing``.

When ``_resolve_indexer`` returns ``None`` (the indexer driver isn't
registered locally), the dispatcher must honour the routing entry's
``FailurePolicy`` rather than silently skipping the op.  This makes
"driver down" and "driver not in this SCOPE" indistinguishable from
the operator's perspective — both routes through the same policy.
"""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import IndexContext
from dynastore.models.protocols.indexing import IndexableOp
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher, IndexerFatal,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry, WriteMode,
)


class _StubRouting:
    def __init__(self, entries):
        self.operations = {Operation.INDEX: entries}


def _dispatcher(entries, *, outbox=None):
    async def routing(c, col):
        return _StubRouting(entries)

    async def registry(driver_id):
        return None

    return IndexDispatcher(
        routing_resolver=routing,
        indexer_registry=registry,
        outbox=outbox,
    )


@pytest.fixture
def ctx():
    return IndexContext(catalog="c", collection="cc")


@pytest.fixture
def op():
    return IndexableOp(
        op_id=uuid4(),
        op="upsert",
        catalog_id="c",
        collection_id="cc",
        driver_instance_id="x",
        item_id="i1",
        payload={"id": "i1"},
        idempotency_key="i1",
    )


@pytest.mark.asyncio
async def test_missing_fatal_raises(ctx, op):
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.FATAL)
    with pytest.raises(IndexerFatal):
        await _dispatcher([entry]).fan_out(ctx, op)


@pytest.mark.asyncio
async def test_missing_warn_logs_once(ctx, op, caplog):
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.WARN)
    d = _dispatcher([entry])
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await d.fan_out(ctx, op)
        await d.fan_out(ctx, op)
    warns = [r for r in caplog.records if "d" in r.message]
    assert len(warns) == 1


@pytest.mark.asyncio
async def test_missing_ignore_silent(ctx, op, caplog):
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.IGNORE)
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await _dispatcher([entry]).fan_out(ctx, op)
    assert not [r for r in caplog.records if "d" in r.message]


@pytest.mark.asyncio
async def test_missing_outbox_enqueues(ctx, op):
    enq = []

    class _Stub:
        async def enqueue_bulk(self, conn, *, catalog_id, rows):
            enq.extend(rows)

    entry = OperationDriverEntry(
        driver_id="d",
        write_mode=WriteMode.ASYNC,
        on_failure=FailurePolicy.OUTBOX,
    )
    await _dispatcher([entry], outbox=_Stub()).fan_out(ctx, op)
    assert len(enq) == 1
    assert enq[0].driver_id == "d"
