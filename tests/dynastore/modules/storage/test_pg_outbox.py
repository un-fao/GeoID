"""Tests for ``storage_outbox`` + ``index_failure_log`` DDL.

Each test runs against a freshly created throwaway PG schema so the
DDL is exercised against an empty namespace. The schema is dropped on
teardown via the writer fixture, which is also the connection that owns
``CREATE SCHEMA`` and so retains drop authority.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from dynastore.tools.identifiers import generate_id_hex


async def _use_fresh_schema(*conns: object) -> str:
    """Create a per-test schema and point every supplied connection at it.

    Works on raw ``asyncpg.Connection`` only — every fixture in this file
    yields one. Returns the schema name so tests can build NOTIFY channels
    that match what ``current_schema()`` will see inside the DDL function.
    """
    schema = f"outbox_t_{generate_id_hex()[:10]}"
    # The first connection owns the schema. Authority to drop it stays
    # with whichever session is the original creator.
    await conns[0].execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    for c in conns:
        await c.execute(f'SET search_path TO "{schema}"')  # type: ignore[attr-defined]
    return schema


async def _drop_schema(conn: object, schema: str) -> None:
    """Best-effort schema teardown."""
    try:
        # Reset search_path first so the DROP doesn't reference the schema
        # we're about to remove via current_schema().
        await conn.execute("RESET search_path")  # type: ignore[attr-defined]
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
    except Exception:
        pass


@pytest.mark.asyncio
async def test_storage_outbox_table_exists(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox

    schema = await _use_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox(async_conn)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name='storage_outbox' AND table_schema=current_schema()"
        )
        assert row is not None
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_drain_index_is_partial(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox

    schema = await _use_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox(async_conn)
        rows = await async_conn.fetch(  # type: ignore[attr-defined]
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname='storage_outbox_drain_idx' AND schemaname=current_schema()"
        )
        assert rows and "WHERE" in rows[0]["indexdef"]
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_ensure_storage_outbox_idempotent(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox

    schema = await _use_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox(async_conn)
        await ensure_storage_outbox(async_conn)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT count(*) AS n FROM information_schema.tables "
            "WHERE table_name='storage_outbox' AND table_schema=current_schema()"
        )
        assert row["n"] == 1
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_index_failure_log_table_exists(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log

    schema = await _use_fresh_schema(async_conn)
    try:
        await ensure_index_failure_log(async_conn)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name='index_failure_log' AND table_schema=current_schema()"
        )
        assert row is not None
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_insert_emits_notify(async_conn, second_async_conn):
    """AFTER INSERT trigger must NOTIFY on outbox_<driver_id>_<schema>."""
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox

    schema = await _use_fresh_schema(async_conn, second_async_conn)
    try:
        await ensure_storage_outbox(async_conn)
        channel = f"outbox_d_{schema}"

        received = asyncio.Event()
        payload_holder: dict[str, str] = {}

        async def _on_notify(conn, pid, ch, payload):  # noqa: ANN001
            payload_holder["p"] = payload
            received.set()

        await second_async_conn.add_listener(channel, _on_notify)  # type: ignore[attr-defined]
        try:
            await async_conn.execute(  # type: ignore[attr-defined]
                "INSERT INTO storage_outbox "
                "(op_id, driver_id, driver_instance_id, collection_id, op, "
                " payload, idempotency_key) "
                "VALUES ($1, 'd', 'di', 'col', 'upsert', '{}'::jsonb, 'k')",
                uuid4(),
            )
            await asyncio.wait_for(received.wait(), timeout=2.0)
        finally:
            await second_async_conn.remove_listener(channel, _on_notify)  # type: ignore[attr-defined]
        assert "driver_id" in payload_holder["p"]
    finally:
        await _drop_schema(async_conn, schema)
