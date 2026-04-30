"""Tests for ``_post_ingest_analyze`` — best-effort planner-stats refresh.

Runs after a successful ingest. Sync + async engines both supported
(production callers pass a sync engine; the helper handles either).
Failures are logged and swallowed so a stats hiccup never demotes a
successful ingest to FAILED.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, AsyncMock, patch

import pytest
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.tasks.ingestion.main_ingestion import _post_ingest_analyze


# ---------------------------------------------------------------------------
# Sync engine path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sync_engine_executes_analyze_with_quoted_schema() -> None:
    engine = MagicMock(spec=Engine)
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn
    engine.begin.side_effect = _begin

    await _post_ingest_analyze(engine, "cat_abc")

    engine.begin.assert_called_once()
    # Verify the executed SQL is ANALYZE with the quoted schema literal
    conn.execute.assert_called_once()
    sql_arg = str(conn.execute.call_args[0][0])
    assert sql_arg == 'ANALYZE "cat_abc"'


@pytest.mark.asyncio
async def test_sync_engine_quotes_schema_with_special_chars() -> None:
    """Identifier should always be wrapped in double quotes — covers
    schemas with reserved words or mixed case."""
    engine = MagicMock(spec=Engine)
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn
    engine.begin.side_effect = _begin

    await _post_ingest_analyze(engine, "MyCatalog")
    sql_arg = str(conn.execute.call_args[0][0])
    assert sql_arg == 'ANALYZE "MyCatalog"'


# ---------------------------------------------------------------------------
# Async engine path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_engine_path_runs_in_async_transaction() -> None:
    engine = MagicMock(spec=AsyncEngine)
    conn = AsyncMock()

    class _AsyncCM:
        async def __aenter__(self_inner):
            return conn
        async def __aexit__(self_inner, *_):
            return False

    engine.begin.return_value = _AsyncCM()

    await _post_ingest_analyze(engine, "cat_xyz")

    conn.execute.assert_awaited_once()
    sql_arg = str(conn.execute.await_args[0][0])
    assert sql_arg == 'ANALYZE "cat_xyz"'


# ---------------------------------------------------------------------------
# Best-effort: failures are swallowed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failure_is_logged_not_raised() -> None:
    """If ANALYZE fails (lock timeout, disk full, anything), the helper
    must NOT raise — ingest success reporting should still go through."""
    engine = MagicMock(spec=Engine)
    engine.begin.side_effect = RuntimeError("boom")

    # Should not raise
    await _post_ingest_analyze(engine, "cat_fail")
    # We get here = no propagation


@pytest.mark.asyncio
async def test_failure_logs_warning_with_schema_name() -> None:
    """Failure log must mention the schema so the operator knows which
    catalog needs autovacuum's catch-up."""
    engine = MagicMock(spec=Engine)
    engine.begin.side_effect = RuntimeError("boom")

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.logger.warning"
    ) as mock_warn:
        await _post_ingest_analyze(engine, "cat_fail")

    mock_warn.assert_called_once()
    # First positional arg is the format string; remaining are interpolated values
    call_args = mock_warn.call_args
    formatted = call_args[0][0] % call_args[0][1:]
    assert "cat_fail" in formatted
    assert "boom" in formatted


# ---------------------------------------------------------------------------
# Success path also logs (for operator visibility)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_success_logs_info_with_schema_name() -> None:
    engine = MagicMock(spec=Engine)
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn
    engine.begin.side_effect = _begin

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.logger.info"
    ) as mock_info:
        await _post_ingest_analyze(engine, "cat_ok")

    # At least one info log should mention the schema
    assert any("cat_ok" in str(c) for c in mock_info.call_args_list)
