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

"""Shared fixtures for ``tests/dynastore/tasks/outbox_drain``.

Mirrors ``tests/dynastore/modules/storage/conftest.py`` — provides a raw
asyncpg connection and a per-test PG schema with ``search_path`` already
set so the DDL helpers and ``PgOutboxStore`` can be exercised directly.
"""
from __future__ import annotations

import os
from typing import AsyncIterator

import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _resolve_asyncpg_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://")


@pytest_asyncio.fixture
async def async_conn() -> AsyncIterator[object]:
    """Raw ``asyncpg.Connection`` against the test database."""
    import asyncpg

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def async_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test PG schema with ``search_path`` already set on ``async_conn``."""
    schema = f"drain_t_{generate_id_hex()[:10]}"
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
