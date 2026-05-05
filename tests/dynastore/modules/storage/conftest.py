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
