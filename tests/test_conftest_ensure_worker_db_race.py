"""Regression tests for #894 — _ensure_worker_db source-DB race.

The defect was that pg_terminate_backend is one-shot: any external client
(dev stack, pgAdmin, leftover sessions) reconnecting between the terminate
and the CREATE DATABASE TEMPLATE call crashed the whole pytest session
with asyncpg.ObjectInUseError. The fix flips ALLOW_CONNECTIONS=false on
the source DB for the terminate+create window and adds a bounded retry
inside that window.
"""

from __future__ import annotations

import inspect

import asyncpg
import pytest

from tests import conftest as ensure_mod


@pytest.mark.asyncio
async def test_retry_succeeds_after_two_object_in_use_then_creates():
    """First two CREATE calls raise ObjectInUseError, third succeeds —
    helper must retry, terminate before each CREATE, and return."""
    calls: list[tuple] = []
    create_attempts = {"n": 0}

    async def execute(query: str, *args):
        calls.append((query, args))
        if query.startswith("SELECT pg_terminate_backend"):
            return None
        if query.startswith("CREATE DATABASE"):
            create_attempts["n"] += 1
            if create_attempts["n"] < 3:
                # Build a minimal ObjectInUseError instance the same way
                # asyncpg does (subclass of PostgresError).
                raise asyncpg.exceptions.ObjectInUseError(
                    "source database \"src\" is being accessed by other users"
                )
            return None
        return None

    await ensure_mod._create_db_template_with_retry(
        execute, "src", "src_gw0", attempts=5, backoff=0.0,
    )

    terminate_calls = [c for c in calls if c[0].startswith("SELECT pg_terminate_backend")]
    create_calls = [c for c in calls if c[0].startswith("CREATE DATABASE")]
    assert len(terminate_calls) == 3, "must re-terminate before each CREATE retry"
    assert len(create_calls) == 3
    assert create_attempts["n"] == 3


@pytest.mark.asyncio
async def test_retry_exhausts_and_raises_runtimeerror_with_context():
    """When every CREATE attempt fails, helper raises RuntimeError that
    surfaces the underlying ObjectInUseError + actionable guidance."""
    async def execute(query: str, *args):
        if query.startswith("CREATE DATABASE"):
            raise asyncpg.exceptions.ObjectInUseError(
                "source database \"src\" is being accessed by other users"
            )
        return None

    with pytest.raises(RuntimeError) as exc_info:
        await ensure_mod._create_db_template_with_retry(
            execute, "src", "src_gw0", attempts=3, backoff=0.0,
        )
    msg = str(exc_info.value)
    assert "src" in msg and "src_gw0" in msg
    assert "3 attempts" in msg
    assert "pg_stat_activity" in msg, (
        "actionable hint about external clients must be in the message"
    )


def test_ensure_worker_db_brackets_create_with_allow_connections_toggle():
    """Source-level guard: _ensure_worker_db must wrap the retry helper
    call with ALLOW_CONNECTIONS=false / true. Without that bracket the
    retry alone cannot win against a busy external client (#894)."""
    src = inspect.getsource(ensure_mod._ensure_worker_db)
    assert "ALLOW_CONNECTIONS false" in src, (
        "must flip ALLOW_CONNECTIONS=false on the source before CREATE"
    )
    assert "ALLOW_CONNECTIONS true" in src, (
        "must restore ALLOW_CONNECTIONS=true after CREATE (in finally)"
    )
    assert "_create_db_template_with_retry" in src, (
        "must call the bounded retry helper, not a single CREATE"
    )
    # The restore must be in the finally clause (last occurrence wins).
    false_idx = src.index("ALLOW_CONNECTIONS false")
    true_idx = src.index("ALLOW_CONNECTIONS true")
    create_idx = src.index("_create_db_template_with_retry")
    assert false_idx < create_idx < true_idx, (
        "ordering must be: ALLOW_CONNECTIONS false -> retry -> "
        "ALLOW_CONNECTIONS true (the true call must be in the finally)"
    )
