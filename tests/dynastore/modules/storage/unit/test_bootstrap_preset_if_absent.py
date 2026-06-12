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

"""Unit tests for ``bootstrap_preset_if_absent``.

The implementation lives in ``modules/presets/bootstrap.py``; the
``lifecycle.py`` shim delegates directly to it.  All DB calls are mocked.
Tests run serially under ``pytest -p no:xdist -p no:logging``.

The two DQL sentinels (``_SELECT_SENTINEL``, ``_INSERT_SENTINEL``) are
module-level objects in ``bootstrap.py``, so tests patch them directly
rather than patching the ``DQLQuery`` class (the old pattern from when
the queries were created inside the function body).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unittest.mock import AsyncMock as _AsyncMock

from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams

# Convenience: patches is_initialized to return False (guard not set) so
# existing tests that don't care about the guard still exercise normal paths.
_guard_unset = patch(
    "dynastore.modules.catalog.bootstrap_guard.is_initialized",
    _AsyncMock(return_value=False),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_preset(name: str = "test_preset") -> MagicMock:
    """Return a minimal Preset mock whose apply returns an AppliedDescriptor."""
    preset = MagicMock()
    preset.name = name
    preset.params_model = NoParams

    async def _apply(params: Any, scope: str, ctx: Any) -> AppliedDescriptor:
        return AppliedDescriptor(payload={"preset_name": name})

    preset.apply = AsyncMock(side_effect=_apply)
    return preset


class _FakeLockAcquired:
    """Advisory lock that succeeds and yields a non-None connection."""

    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> MagicMock:
        return MagicMock()  # non-None → lock acquired

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FakeLockTimeout:
    """Advisory lock that yields None — simulates a lock timeout."""

    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> None:
        return None  # lock timeout

    async def __aexit__(self, *_: Any) -> bool:
        return False


def _make_sentinel_mocks(select_returns: Any = None, insert_returns: Any = None) -> tuple:
    """Return ``(select_mock, insert_mock)`` sentinels for patching bootstrap.py."""
    select_mock = MagicMock()
    select_mock.execute = AsyncMock(return_value=select_returns)
    insert_mock = MagicMock()
    insert_mock.execute = AsyncMock(return_value=insert_returns)
    return select_mock, insert_mock


# ---------------------------------------------------------------------------
# Test: sentinel absent → preset applied and sentinel inserted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_applies_preset_when_sentinel_absent() -> None:
    """When no sentinel row exists, apply() is called and the sentinel is inserted."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    preset = _make_preset("default_roles_baseline")
    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=None)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="default_roles_baseline"
        )

    assert result is True
    sel_mock.execute.assert_awaited_once()
    ins_mock.execute.assert_awaited_once()
    preset.apply.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test: sentinel present → no-op, apply never called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_noop_when_sentinel_present() -> None:
    """When a sentinel row already exists, apply() is never called."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    preset = _make_preset("iam_baseline")
    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=(1,))

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="iam_baseline"
        )

    assert result is False
    preset.apply.assert_not_called()
    ins_mock.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Test: force=True → re-applies even when sentinel present (self-heal)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_force_reapplies_when_sentinel_present() -> None:
    """force=True re-applies a load-bearing preset despite an existing sentinel.

    This is the /health self-heal path: a DB whose `unauthenticated` role lost
    `public_access` still has the public_access_baseline sentinel, so the normal
    path would skip and the probe would 403 forever. force=True must re-run
    apply() to restore the grant, and the sentinel INSERT (ON CONFLICT DO
    NOTHING) is still issued.
    """
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")
    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=(1,))

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline", force=True
        )

    assert result is True
    preset.apply.assert_awaited_once()
    sel_mock.execute.assert_awaited_once()
    ins_mock.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test: advisory lock timeout → returns False without apply
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_returns_false_on_lock_timeout() -> None:
    """When advisory lock times out (conn=None), returns False without applying."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockTimeout,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline"
        )

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# Test: apply() raises → exception propagates (lock already released by ctx mgr)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_propagates_apply_failure() -> None:
    """If preset.apply() raises, the exception propagates to the caller."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    broken_preset = MagicMock()
    broken_preset.name = "broken_preset"
    broken_preset.params_model = NoParams
    broken_preset.apply = AsyncMock(side_effect=RuntimeError("apply exploded"))

    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=None)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=broken_preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), patch(
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=False),
    ):
        with pytest.raises(RuntimeError, match="apply exploded"):
            await bootstrap_preset_if_absent(
                MagicMock(), preset_name="broken_preset"
            )


# ---------------------------------------------------------------------------
# Bootstrap guard integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bootstrap_guard_set_skips_non_force_preset() -> None:
    """When the bootstrap guard is set, non-force presets are skipped entirely."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
    from unittest.mock import AsyncMock

    preset = _make_preset("default_roles_baseline")

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=True),
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="default_roles_baseline"
        )

    assert result is False
    preset.apply.assert_not_called()


@pytest.mark.asyncio
async def test_bootstrap_guard_set_force_still_applies() -> None:
    """force=True bypasses the bootstrap guard — public_access_baseline self-heal path."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
    from unittest.mock import AsyncMock

    preset = _make_preset("public_access_baseline")
    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=(1,))

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), patch(
        # Guard is set, but force=True must bypass it.
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=True),
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline", force=True
        )

    # Guard is set but force=True: preset must have applied.
    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_bootstrap_guard_unset_applies_normally() -> None:
    """When the guard is not set, bootstrap logic applies the preset as usual."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
    from unittest.mock import AsyncMock

    preset = _make_preset("iam_baseline")
    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=None)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), patch(
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=False),
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="iam_baseline"
        )

    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_bootstrap_failure_leaves_guard_unset() -> None:
    """If apply() raises, the exception propagates — guard stays unset for retry."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
    from unittest.mock import AsyncMock

    broken_preset = MagicMock()
    broken_preset.name = "iam_baseline"
    broken_preset.params_model = NoParams
    broken_preset.apply = AsyncMock(side_effect=RuntimeError("db locked"))

    sel_mock, ins_mock = _make_sentinel_mocks(select_returns=None)

    mark_called = False

    async def _fake_mark(db_resource=None):
        nonlocal mark_called
        mark_called = True

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=broken_preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), patch(
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=False),
    ):
        with pytest.raises(RuntimeError, match="db locked"):
            await bootstrap_preset_if_absent(MagicMock(), preset_name="iam_baseline")

    # mark_initialized is called from modules/__init__.py, not lifecycle.py,
    # so it must NOT be called here on failure.
    assert not mark_called
