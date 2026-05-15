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

"""Pin that the Valkey apply handler registers EVEN on the legacy fallback path.

Regression cover for #818: pre-fix, ``CacheModule.lifespan`` wrapped both
``register_apply_handler`` and ``unregister_apply_handler`` inside ``if
engine_mode:``.  On Cloud Run the very first ``build_engine_snapshot`` is
guaranteed to return empty (DBService is priority 10, runs *after*
DBConfigModule at priority 0 → ``db_resource is None``), so
``engine_cache.get("valkey_engine")`` raises ``KeyError`` and
``engine_mode`` evaluates False.  Under the old gating, the apply handler
silently went unregistered for the lifetime of the process, so every
subsequent ``PUT /configs/plugins/valkey_engine_config`` returned 200 but
did not trigger a live reconnect — the runtime-tunable contract
advertised by #633 / #724 / #743 was silently broken.

The fix drops the ``if engine_mode:`` guard.  These tests assert the
register/unregister symmetry holds on the legacy fallback path so the
guard can't quietly come back.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.cache.cache_module import CacheModule, _on_valkey_engine_config_change
from dynastore.modules.db_config.engine_config import ValkeyEngineConfig


@pytest.mark.asyncio
async def test_apply_handler_registered_on_legacy_fallback_path(monkeypatch):
    """Legacy path (no engine_cache on app_state) MUST still register the
    apply handler, otherwise PUTs to /configs persist but produce no
    live reconnect — the silent-tunable regression that #818 fixes.
    """
    monkeypatch.setenv("VALKEY_URL", "valkey://10.0.0.1:6379")
    monkeypatch.setenv("VALKEY_CLUSTER", "false")

    # Pretend deps are installed so we enter the real legacy branch.
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)

    # Stub the backend so we don't touch network on import / probe.
    fake_backend = MagicMock()
    fake_backend.info = AsyncMock(
        return_value={
            "server": {"redis_version": "7.2.4", "redis_mode": "standalone"},
            "memory": {"used_memory_human": "1M"},
        }
    )
    fake_backend.close = AsyncMock(return_value=None)

    manager = MagicMock()
    manager.register_backend = MagicMock()
    manager.unregister_backend = MagicMock()

    # app_state with no engine_cache → forces engine_mode=False (legacy path).
    app_state = MagicMock()
    app_state.engine_cache = None

    pre_handlers = list(ValkeyEngineConfig.get_apply_handlers())

    with (
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend",
            return_value=fake_backend,
        ),
        patch(
            "dynastore.tools.cache.get_cache_manager",
            return_value=manager,
        ),
    ):
        module = CacheModule(app_state=app_state)
        async with module.lifespan(app_state):
            inside_handlers = list(ValkeyEngineConfig.get_apply_handlers())
            assert _on_valkey_engine_config_change in inside_handlers, (
                "apply handler must be registered on the legacy fallback "
                "path; without it, PUT /configs/plugins/valkey_engine_config "
                "persists the change but no live reconnect fires (#818)."
            )

    post_handlers = list(ValkeyEngineConfig.get_apply_handlers())
    assert _on_valkey_engine_config_change not in post_handlers, (
        "apply handler must be unregistered on lifespan exit; otherwise "
        "stale handlers leak across process-restart-equivalent test runs."
    )
    # Symmetry: registry returns to its pre-lifespan state.
    assert post_handlers == pre_handlers
