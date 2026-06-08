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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Shared fixtures for ``tests/dynastore/modules/storage``.

Provides raw asyncpg connections so we can exercise LISTEN / NOTIFY,
SKIP LOCKED claims, and other PG-native semantics that the SQLAlchemy
``AsyncConnection`` doesn't surface directly. Tests that need a SA
connection should use the project-wide ``db_engine`` / ``app_lifespan``
fixtures instead.
"""
from __future__ import annotations

import os
from typing import AsyncIterator

import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _resolve_asyncpg_url() -> str:
    """Return a libpq-style URL safe for ``asyncpg.connect``.

    Mirrors the project-wide test default but strips the ``+asyncpg``
    SQLAlchemy dialect suffix when present. Picks up an isolated per-worker
    database under xdist via ``DATABASE_URL`` exported by the top-level
    conftest.
    """
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://")


@pytest_asyncio.fixture
async def async_conn() -> AsyncIterator[object]:
    """Raw ``asyncpg.Connection`` against the test database.

    Each test gets its own physical connection — required so the listener
    in ``second_async_conn`` stays distinct from the writer here, which is
    how PG NOTIFY actually delivers across sessions.
    """
    import asyncpg

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def second_async_conn() -> AsyncIterator[object]:
    """A second raw ``asyncpg.Connection`` on the same DB.

    Used to exercise SKIP LOCKED isolation between claimers and to listen
    on NOTIFY while the primary connection writes.
    """
    import asyncpg

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def async_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test PG schema with ``search_path`` already set on ``async_conn``.

    Returns the bare schema name string. Tests that need a second connection
    to also see the schema must set ``search_path`` on it themselves (see
    ``test_pg_outbox_claim_batch_skip_locked``). Schema is dropped on
    teardown by the same connection that created it.
    """
    schema = f"outbox_t_{generate_id_hex()[:10]}"
    await async_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    await async_conn.execute(f'SET search_path TO "{schema}"')  # type: ignore[attr-defined]
    try:
        yield schema
    finally:
        try:
            await async_conn.execute("RESET search_path")  # type: ignore[attr-defined]
            await async_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
        except Exception:
            pass
