#    Copyright 2025 FAO
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

"""
CacheModule — registers a shared Valkey cache backend when VALKEY_URL is set.

Falls back to local in-memory cache (LocalAsyncCacheBackend, priority=1000)
when Valkey is unavailable or VALKEY_URL is not configured.

Configuration (via PluginConfig framework, not env vars):
  - probe_timeout_seconds: timeout for backend.info() probe (default 5s)
  - socket_connect_timeout_seconds: timeout per socket connection (default 10s)
  - circuit_breaker_threshold: failures before fallback (default 3)

Access via: /api/catalog/v2/configs?plugin_id=module_cache

Add ``module_cache`` to the deployment scope extras to activate::

    scope_catalog = ["dynastore[...,module_cache]"]
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)


async def _load_cache_config() -> "CachePluginConfig":
    """Load cache config from PluginConfig protocol.

    Falls back to defaults if config is missing or protocol unavailable
    (e.g., during early bootstrap before ConfigProtocol is registered).
    """
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        from dynastore.models.protocols.configs import ConfigsProtocol

        try:
            from dynastore.frameworks.plugin import get_protocol
            configs_proto = get_protocol(ConfigsProtocol)
        except Exception as e:
            logger.debug("CacheModule: ConfigsProtocol not available yet (%s), using defaults", e)
            return CachePluginConfig()

        try:
            cfg = await configs_proto.get_config(CachePluginConfig)
            if cfg:
                return cfg
        except Exception as e:
            logger.debug("CacheModule: failed to load CachePluginConfig (%s), using defaults", e)

    except Exception as e:
        logger.debug("CacheModule: config protocol unavailable (%s), using defaults", e)

    from dynastore.modules.cache.cache_config import CachePluginConfig
    return CachePluginConfig()  # Return defaults


class CacheModule(ModuleProtocol):
    """SCOPE-controlled module that wires Valkey as the shared cache backend.

    Priority 9 — starts before DBService (10) so the backend is registered
    before any module that uses ``@cached`` in its lifespan.
    """

    priority: int = 9

    def __init__(self, app_state: object) -> None:
        self.app_state = app_state

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        valkey_url = os.getenv("VALKEY_URL")
        if not valkey_url:
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "VALKEY_URL not set; cross-instance consistency NOT guaranteed."
            )
            yield
            return

        # Load cache config (timeouts, circuit breaker, etc.)
        cache_cfg = await _load_cache_config()

        # Mask credentials in logged URL (valkey://:pass@host → valkey://host)
        _safe_url = valkey_url.split("@")[-1] if "@" in valkey_url else valkey_url

        _tls = os.getenv("VALKEY_TLS", "").lower() in ("1", "true", "yes")
        _iam = os.getenv("VALKEY_IAM_AUTH", "").lower() in ("1", "true", "yes")
        _cluster = os.getenv("VALKEY_CLUSTER", "").lower() in ("1", "true", "yes")
        logger.info(
            "CacheModule: Connecting to Valkey at %s (tls=%s, iam_auth=%s, cluster=%s, probe_timeout=%ss) …",
            _safe_url, _tls, _iam, _cluster, cache_cfg.probe_timeout_seconds,
        )
        try:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend
            backend = ValkeyCacheBackend(
                url=valkey_url,
                socket_connect_timeout=cache_cfg.socket_connect_timeout_seconds,
                circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
            )
        except Exception as exc:
            logger.warning(
                "CacheModule: Cannot initialise Valkey backend (%s) — falling back to local cache.",
                exc,
            )
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "Valkey unavailable; cross-instance consistency NOT guaranteed."
            )
            yield
            return

        try:
            info = await asyncio.wait_for(backend.info(), timeout=cache_cfg.probe_timeout_seconds)
            version = info.get("server", {}).get("redis_version", "?")
            mode = info.get("server", {}).get("redis_mode", "standalone")
            used_mb = info.get("memory", {}).get("used_memory_human", "?")
            logger.info(
                "CacheModule: Valkey OK — version=%s mode=%s used_memory=%s host=%s",
                version, mode, used_mb, _safe_url,
            )
        except Exception as exc:
            _reason = "probe timed out" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            logger.warning(
                "CacheModule: Valkey unreachable at %s (%s) — falling back to local cache.",
                _safe_url, _reason,
            )
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "Valkey connection failed; cross-instance consistency NOT guaranteed."
            )
            await backend.close()
            yield
            return

        from dynastore.tools.cache import _notify_backend_upgrade, get_cache_manager

        get_cache_manager().register_backend(backend)
        _notify_backend_upgrade()
        logger.info(
            "CACHE BACKEND: VALKEY (shared, cross-instance) — host=%s version=%s mode=%s used_memory=%s",
            _safe_url, version, mode, used_mb,
        )

        try:
            yield
        finally:
            await backend.close()
            logger.info("CacheModule: Valkey connection closed.")
