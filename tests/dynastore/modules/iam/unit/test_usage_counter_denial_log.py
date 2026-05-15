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

"""Pin the ``usage_counter_denied`` structured-log emission.

Two invariants:

1. **Format** — the WARNING line carries ``condition_type``,
   ``policy_id``, ``principal_key``, ``scope``, ``count``, ``limit``, and
   ``window_seconds`` so a single GCP log-based metric can alert on the
   ``usage_counter_denied`` token.
2. **Throttle** — at sustained denial rates the gate emits at most one
   line per ``(condition_type, policy_id, principal_key)`` per 60 s. A
   tight loop must not flood logs.
"""

from __future__ import annotations

import logging
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.iam import conditions as conditions_mod
from dynastore.modules.iam.conditions import (
    EvaluationContext,
    MaxCountHandler,
    RateLimitHandler,
)
from dynastore.modules.iam.exceptions import (
    QuotaExceededError,
    RateLimitExceededError,
)


@pytest.fixture(autouse=True)
def _clear_denial_gate():
    """Each test starts with an empty throttle so prior denials don't
    suppress the line under test."""
    conditions_mod._DENIAL_LOG_GATE.clear()
    yield
    conditions_mod._DENIAL_LOG_GATE.clear()


def _ctx(principal_id: str = "alice") -> EvaluationContext:
    return EvaluationContext(
        request=None,
        storage=None,  # type: ignore[arg-type]
        principal_id=principal_id,
        path="/anything",
        method="GET",
    )


def _rate_config() -> Dict[str, Any]:
    return {
        "limit": 5,
        "window_seconds": 60,
        "scope": "principal",
        "_policy_id": "p-rate",
    }


def _quota_config() -> Dict[str, Any]:
    return {
        "limit": 10,
        "scope": "principal",
        "_policy_id": "p-rate",
    }


@pytest.mark.asyncio
async def test_rate_limit_denial_emits_structured_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """RateLimitHandler must emit a WARNING with the canonical token and
    every field a log-based metric / SRE alert needs."""
    counter = AsyncMock()
    counter.incr_if_below = AsyncMock(return_value=(7, False))

    caplog.set_level(logging.WARNING, logger="dynastore.modules.iam.conditions")

    with patch.object(conditions_mod, "get_protocol", return_value=counter):
        with pytest.raises(RateLimitExceededError):
            await RateLimitHandler().evaluate(_rate_config(), _ctx())

    denial_lines = [
        r for r in caplog.records if "usage_counter_denied" in r.getMessage()
    ]
    assert len(denial_lines) == 1, "exactly one structured WARNING expected"

    msg = denial_lines[0].getMessage()
    for token in (
        "usage_counter_denied",
        "condition_type=rate_limit",
        "policy_id=p-rate",
        "principal_key=alice",
        "scope=principal",
        "count=7",
        "limit=5",
        "window_seconds=60",
    ):
        assert token in msg, f"missing token {token!r} in: {msg!r}"


@pytest.mark.asyncio
async def test_max_count_denial_emits_lifetime_window(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """MaxCountHandler is lifetime — ``window_seconds=lifetime`` so the
    metric can split rate vs. quota denials by the same field name."""
    counter = AsyncMock()
    counter.incr_if_below = AsyncMock(return_value=(11, False))

    caplog.set_level(logging.WARNING, logger="dynastore.modules.iam.conditions")

    with patch.object(conditions_mod, "get_protocol", return_value=counter):
        with pytest.raises(QuotaExceededError):
            await MaxCountHandler().evaluate(_quota_config(), _ctx())

    denial_lines = [
        r for r in caplog.records if "usage_counter_denied" in r.getMessage()
    ]
    assert len(denial_lines) == 1
    msg = denial_lines[0].getMessage()
    assert "condition_type=max_count" in msg
    assert "count=11" in msg
    assert "limit=10" in msg
    assert "window_seconds=lifetime" in msg


@pytest.mark.asyncio
async def test_denial_log_is_throttled_per_principal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """50 consecutive denials for the same ``(policy, principal)`` must
    emit one WARNING — the throttle is the whole point of the gate."""
    counter = AsyncMock()
    counter.incr_if_below = AsyncMock(return_value=(99, False))

    caplog.set_level(logging.WARNING, logger="dynastore.modules.iam.conditions")

    handler = RateLimitHandler()
    config = _rate_config()
    ctx = _ctx()

    with patch.object(conditions_mod, "get_protocol", return_value=counter):
        for _ in range(50):
            with pytest.raises(RateLimitExceededError):
                await handler.evaluate(config, ctx)

    denial_lines = [
        r for r in caplog.records if "usage_counter_denied" in r.getMessage()
    ]
    assert len(denial_lines) == 1, (
        f"expected one throttled WARNING across 50 denials, got "
        f"{len(denial_lines)}"
    )


@pytest.mark.asyncio
async def test_denial_log_distinguishes_principals(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Throttle is per ``(condition_type, policy_id, principal_key)`` —
    a different principal hitting the same policy must still log."""
    counter = AsyncMock()
    counter.incr_if_below = AsyncMock(return_value=(6, False))

    caplog.set_level(logging.WARNING, logger="dynastore.modules.iam.conditions")

    handler = RateLimitHandler()
    config = _rate_config()

    with patch.object(conditions_mod, "get_protocol", return_value=counter):
        for principal in ("alice", "bob", "carol"):
            with pytest.raises(RateLimitExceededError):
                await handler.evaluate(config, _ctx(principal_id=principal))

    denial_lines = [
        r for r in caplog.records if "usage_counter_denied" in r.getMessage()
    ]
    assert len(denial_lines) == 3
    keys = {
        next(
            tok.split("=", 1)[1]
            for tok in r.getMessage().split()
            if tok.startswith("principal_key=")
        )
        for r in denial_lines
    }
    assert keys == {"alice", "bob", "carol"}


@pytest.mark.asyncio
async def test_rate_limit_allow_does_not_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Allowed requests must not touch the denial log channel — verifies
    we did not regress on log volume for the happy path."""
    counter = AsyncMock()
    counter.incr_if_below = AsyncMock(return_value=(1, True))

    caplog.set_level(logging.WARNING, logger="dynastore.modules.iam.conditions")

    with patch.object(conditions_mod, "get_protocol", return_value=counter):
        result = await RateLimitHandler().evaluate(_rate_config(), _ctx())

    assert result is True
    denial_lines = [
        r for r in caplog.records if "usage_counter_denied" in r.getMessage()
    ]
    assert denial_lines == []
