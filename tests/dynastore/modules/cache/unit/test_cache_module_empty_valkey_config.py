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

"""CacheModule lifespan must NOT crash on empty Valkey configuration.

Issue #1026 — under integration tests, notebooks, demos, fresh installs,
neither ``VALKEY_URL`` nor ``VALKEY_DISCOVERY_HOST`` is set. When
``engine_cache.get("valkey_engine")`` is invoked the registered
``ValkeyEngineConfig.engine_init`` calls ``build_valkey_client`` which
raises ``ValueError("`url` is required.")``. Before this fix only
``KeyError`` and the "disabled" ``RuntimeError`` were caught, so the
ValueError aborted the entire FastAPI lifespan and cascaded into 500s
for every priority-≥9 module.

The fix at ``cache_module.py`` extends the catch block to also handle
``ValueError`` and logs a WARNING so misconfigured production
deployments are still visible.
"""

from __future__ import annotations

import logging

from dynastore.modules.cache.cache_module import CacheModule


class _RaisingEngineCache:
    """Stub mirroring ``EngineInstanceCache.get`` for ``valkey_engine``.

    Raises ``ValueError`` exactly the way ``build_valkey_client`` does
    when neither ``connection_url`` nor ``discovery_host`` is set.
    """

    async def get(self, name: str):  # noqa: D401 - test stub
        assert name == "valkey_engine"
        raise ValueError(
            "build_valkey_client(cluster_mode=False): `url` is required."
        )


class _AppState:
    """Minimal app_state stub with an engine_cache present."""

    def __init__(self) -> None:
        self.engine_cache = _RaisingEngineCache()


async def test_lifespan_falls_back_when_valkey_config_empty(
    monkeypatch, caplog
):
    """Empty Valkey config → ValueError → fall through to local cache.

    Pins issue #1026: a ``ValueError`` from
    ``ValkeyEngineConfig().engine_init()`` must NOT abort the lifespan.
    Lifespan yields cleanly (local in-memory cache) and a WARNING is
    logged so the misconfiguration is observable.
    """
    monkeypatch.delenv("VALKEY_URL", raising=False)
    monkeypatch.delenv("VALKEY_DISCOVERY_HOST", raising=False)

    # Pretend the ``module_cache`` extra IS installed so we go down the
    # engine_cache.get path (not the early _CACHE_DEPS_OK skip).
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)

    app_state = _AppState()
    module = CacheModule(app_state=app_state)

    entered = False
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.cache.cache_module"):
        async with module.lifespan(app_state):
            entered = True

    assert entered, "lifespan must yield (no exception) on empty Valkey config"

    # WARNING was logged so misconfigured prod is visible.
    misconfig_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "misconfigured" in r.getMessage().lower()
    ]
    assert misconfig_warnings, (
        "expected a WARNING log mentioning the misconfiguration; "
        f"records: {[r.getMessage() for r in caplog.records]}"
    )
