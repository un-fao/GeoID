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
Dynamic CORS middleware that reads allowed origins from
PlatformConfigsProtocol at runtime so changes take effect
without a restart.
"""

import logging
import time
from typing import List, Optional

from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from dynastore.modules.apikey.security_config import (
    SECURITY_PLUGIN_CONFIG_ID,
    SecurityPluginConfig,
)

logger = logging.getLogger(__name__)

# How often (seconds) to re-read config from the protocol.
_RELOAD_INTERVAL = 30


class DynamicCORSMiddleware:
    """
    Wraps Starlette's CORSMiddleware but rebuilds it when the
    SecurityPluginConfig changes (checked every _RELOAD_INTERVAL seconds).
    """

    def __init__(self, app: ASGIApp):
        self.app = app
        self._inner: Optional[CORSMiddleware] = None
        self._last_reload: float = 0.0
        self._last_config_hash: Optional[int] = None
        # Build an initial middleware with defaults so the first request works.
        self._rebuild(SecurityPluginConfig())

    # ------------------------------------------------------------------

    def _rebuild(self, cfg: SecurityPluginConfig) -> None:
        allow_credentials = cfg.cors_allowed_origins != ["*"]
        self._inner = CORSMiddleware(
            app=self.app,
            allow_origins=cfg.cors_allowed_origins,
            allow_credentials=allow_credentials,
            allow_methods=cfg.cors_allow_methods,
            allow_headers=cfg.cors_allow_headers,
            max_age=cfg.cors_max_age,
        )
        self._last_config_hash = hash(
            (
                tuple(cfg.cors_allowed_origins),
                tuple(cfg.cors_allow_methods),
                tuple(cfg.cors_allow_headers),
                cfg.cors_max_age,
            )
        )

    async def _maybe_reload(self) -> None:
        now = time.monotonic()
        if now - self._last_reload < _RELOAD_INTERVAL:
            return
        self._last_reload = now

        try:
            from dynastore.modules import get_protocol
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

            svc = get_protocol(PlatformConfigsProtocol)
            if svc is None:
                return
            cfg = await svc.get_config(SECURITY_PLUGIN_CONFIG_ID)
            if not isinstance(cfg, SecurityPluginConfig):
                cfg = SecurityPluginConfig.model_validate(
                    cfg.model_dump() if hasattr(cfg, "model_dump") else {}
                )
        except Exception:
            logger.debug("DynamicCORSMiddleware: config reload skipped (service not ready)")
            return

        cfg_hash = hash(
            (
                tuple(cfg.cors_allowed_origins),
                tuple(cfg.cors_allow_methods),
                tuple(cfg.cors_allow_headers),
                cfg.cors_max_age,
            )
        )
        if cfg_hash != self._last_config_hash:
            logger.info("DynamicCORSMiddleware: CORS config changed, rebuilding middleware")
            self._rebuild(cfg)

    # ------------------------------------------------------------------

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in ("http", "websocket"):
            await self._maybe_reload()
        await self._inner(scope, receive, send)  # type: ignore[union-attr]
