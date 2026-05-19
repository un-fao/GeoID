"""Retry behavior for ``ensure_db_extension`` against transient connection drops.

Foundational DDL like ``CREATE EXTENSION postgis`` runs during DatastoreModule's
lifespan before the app accepts traffic. Dev compositions reset the DB
mid-startup (``db_entrypoint_dev.sh``), which surfaces as
``OperationalError: server closed the connection unexpectedly`` with
``connection_invalidated=True``. The retry should swallow up to N-1 of these,
not the generic ``OperationalError`` (which would mask real errors), and not
any other exception class.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from dynastore.modules.db_config import maintenance_tools


def _make_invalidated_op_error() -> SAOperationalError:
    err = SAOperationalError(
        statement="CREATE EXTENSION",
        params={},
        orig=Exception("server closed the connection unexpectedly"),
    )
    err.connection_invalidated = True  # type: ignore[attr-defined]
    return err


def _make_other_op_error() -> SAOperationalError:
    err = SAOperationalError(
        statement="CREATE EXTENSION",
        params={},
        orig=Exception("syntax error"),
    )
    err.connection_invalidated = False  # type: ignore[attr-defined]
    return err


@pytest.mark.asyncio
async def test_retries_then_succeeds_on_invalidated_connection():
    execute_mock = AsyncMock(side_effect=[_make_invalidated_op_error(), None])
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 2


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts():
    execute_mock = AsyncMock(side_effect=[
        _make_invalidated_op_error(),
        _make_invalidated_op_error(),
        _make_invalidated_op_error(),
    ])
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(SAOperationalError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 3


@pytest.mark.asyncio
async def test_does_not_retry_on_non_invalidated_operational_error():
    execute_mock = AsyncMock(side_effect=_make_other_op_error())
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(SAOperationalError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 1


@pytest.mark.asyncio
async def test_does_not_retry_on_unrelated_exception():
    execute_mock = AsyncMock(side_effect=RuntimeError("boom"))
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(RuntimeError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 1
