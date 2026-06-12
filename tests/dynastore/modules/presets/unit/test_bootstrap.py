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

"""Unit tests for ``modules/presets/bootstrap.bootstrap_preset_if_absent``.

These tests verify the neutral-module implementation directly, independent of
the shim in ``modules/storage/presets/lifecycle.py``.

All DB calls are mocked.  Tests run serially (-p no:randomly -n0).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams


# ---------------------------------------------------------------------------
# Helpers shared with lifecycle shim tests
# ---------------------------------------------------------------------------

def _make_preset(name: str = "test_preset") -> MagicMock:
    preset = MagicMock()
    preset.name = name
    preset.params_model = NoParams

    async def _apply(params: Any, scope: str, ctx: Any) -> AppliedDescriptor:
        return AppliedDescriptor(payload={"preset_name": name})

    preset.apply = AsyncMock(side_effect=_apply)
    return preset


class _FakeLockAcquired:
    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> MagicMock:
        return MagicMock()

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FakeLockTimeout:
    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> bool:
        return False


_guard_unset = patch(
    "dynastore.modules.catalog.bootstrap_guard.is_initialized",
    AsyncMock(return_value=False),
)


# ---------------------------------------------------------------------------
# bootstrap.py-level tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_neutral_bootstrap_applies_when_sentinel_absent() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    execute_calls: list = []

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            execute_calls.append(kw)
            return None  # SELECT absent on first call; INSERT returns None too

    preset = _make_preset("default_roles_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         _guard_unset:
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="default_roles_baseline")

    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_neutral_bootstrap_guard_set_skips_non_force() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    preset = _make_preset("default_roles_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.catalog.bootstrap_guard.is_initialized", AsyncMock(return_value=True)):
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="default_roles_baseline")

    assert result is False
    preset.apply.assert_not_called()


@pytest.mark.asyncio
async def test_neutral_bootstrap_force_bypasses_guard() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    execute_calls: list = []

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            execute_calls.append(kw)
            return (1,) if not execute_calls[1:] else None

    preset = _make_preset("public_access_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         patch("dynastore.modules.catalog.bootstrap_guard.is_initialized", AsyncMock(return_value=True)):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline", force=True
        )

    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_neutral_bootstrap_lock_timeout_returns_false() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockTimeout), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset):
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="public_access_baseline")

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# Shim tests: lifecycle.bootstrap_preset_if_absent delegates to neutral module
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lifecycle_shim_delegates_to_neutral_module() -> None:
    """The shim in lifecycle.py returns an awaitable that calls the neutral impl."""
    called_with: list = []

    async def _fake_neutral(engine: Any, *, preset_name: str, scope_key: str,
                             lock_key: Any, force: bool) -> bool:
        called_with.append((preset_name, scope_key, force))
        return True

    with patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        _fake_neutral,
    ):
        from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent as _shim
        result = await _shim(MagicMock(), preset_name="some_preset", force=False)

    assert result is True
    assert called_with == [("some_preset", "platform", False)]


# ---------------------------------------------------------------------------
# IAM preset registration (smoke test — no DB)
# ---------------------------------------------------------------------------

def test_iam_presets_registered_in_registry() -> None:
    """Importing the IAM presets package registers both presets into the registry."""
    import dynastore.modules.iam.presets  # noqa: F401 — side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    assert get_preset("default_roles_baseline") is not None
    assert get_preset("public_access_baseline") is not None
