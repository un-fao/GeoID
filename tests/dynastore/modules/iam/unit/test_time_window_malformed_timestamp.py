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

"""Pin the malformed-timestamp handling of TimeWindowHandler.

Three invariants:

1. **In-window** — a valid ``start`` in the past passes without error.
2. **Out-of-window** — a valid ``start`` in the future raises ``IamError``
   so the key is rejected (the propagation must NOT be swallowed).
3. **Malformed timestamp** — an unparseable ``start`` or ``end`` value
   logs an ERROR naming the offending bound and value, then raises
   ``IamError`` (fail closed: a policy with an unparseable validity bound
   must never grant access).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from dynastore.modules.iam.conditions import EvaluationContext, TimeWindowHandler
from dynastore.modules.iam.exceptions import IamError

_LOGGER_NAME = "dynastore.modules.iam.conditions"


def _ctx() -> EvaluationContext:
    return EvaluationContext(
        request=MagicMock(),
        storage=None,  # type: ignore[arg-type]
        principal_id="user_test",
        path="/any",
        method="GET",
    )


def _utc_iso(delta: timedelta) -> str:
    return (datetime.now(timezone.utc) + delta).isoformat()


@pytest.mark.asyncio
async def test_in_window_start_in_past_passes() -> None:
    """A ``start`` timestamp in the past must not raise."""
    config: Dict[str, Any] = {
        "start": _utc_iso(timedelta(hours=-1)),
        # No end; hour-window defaults cover the full day.
        "start_hour": 0,
        "end_hour": 24,
    }
    handler = TimeWindowHandler()
    result = await handler.evaluate(config, _ctx())
    assert result is True


@pytest.mark.asyncio
async def test_out_of_window_start_in_future_raises_iam_error() -> None:
    """A ``start`` timestamp in the future must raise ``IamError``.

    This verifies the time-window rejection path still propagates after the
    try/except restructuring — IamError must escape the handler, not be
    swallowed.
    """
    config: Dict[str, Any] = {
        "start": _utc_iso(timedelta(hours=+1)),
        "start_hour": 0,
        "end_hour": 24,
    }
    handler = TimeWindowHandler()
    with pytest.raises(IamError, match="valid from"):
        await handler.evaluate(config, _ctx())


@pytest.mark.asyncio
async def test_out_of_window_end_in_past_raises_iam_error() -> None:
    """An ``end`` timestamp in the past must raise ``IamError``."""
    config: Dict[str, Any] = {
        "end": _utc_iso(timedelta(hours=-1)),
        "start_hour": 0,
        "end_hour": 24,
    }
    handler = TimeWindowHandler()
    with pytest.raises(IamError, match="expired at"):
        await handler.evaluate(config, _ctx())


@pytest.mark.asyncio
async def test_malformed_start_logs_error_and_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unparseable ``start`` value must log an ERROR and raise ``IamError``.

    Fail closed: a policy whose validity bound cannot be parsed must reject
    the request, never silently grant access. The log line must name the
    offending value so operators can locate the broken policy.
    """
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)
    config: Dict[str, Any] = {
        "start": "not-a-date",
        "start_hour": 0,
        "end_hour": 24,
    }
    handler = TimeWindowHandler()
    with pytest.raises(IamError, match="'start' date format"):
        await handler.evaluate(config, _ctx())

    error_lines = [
        r for r in caplog.records
        if r.levelno == logging.ERROR and "start" in r.getMessage()
    ]
    assert len(error_lines) >= 1, "expected at least one ERROR for malformed 'start'"
    msg = error_lines[0].getMessage()
    assert "not-a-date" in msg, f"offending value missing from error log: {msg!r}"


@pytest.mark.asyncio
async def test_malformed_end_logs_error_and_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unparseable ``end`` value must log an ERROR and raise ``IamError``."""
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)
    config: Dict[str, Any] = {
        "end": "INVALID-TIMESTAMP",
        "start_hour": 0,
        "end_hour": 24,
    }
    handler = TimeWindowHandler()
    with pytest.raises(IamError, match="'end' date format"):
        await handler.evaluate(config, _ctx())

    error_lines = [
        r for r in caplog.records
        if r.levelno == logging.ERROR and "end" in r.getMessage()
    ]
    assert len(error_lines) >= 1, "expected at least one ERROR for malformed 'end'"
    msg = error_lines[0].getMessage()
    assert "INVALID-TIMESTAMP" in msg, f"offending value missing from error log: {msg!r}"
