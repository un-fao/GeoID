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
Push-based dynamic CORS middleware.

Rebuilds the inner Starlette CORSMiddleware when SecurityPluginConfig
changes via the PluginConfig apply handler on_apply callback -- no polling.
"""

import logging
from typing import Optional

from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from dynastore.modules.iam.security_config import (
    SECURITY_PLUGIN_CONFIG_ID,
    SecurityPluginConfig,
)

logger = logging.getLogger(__name__)

# Module-level reference so the on_apply callback can reach it.
_cors_instance: Optional["DynamicCORSMiddleware"] = None


class DynamicCORSMiddleware:
    """Wraps Starlette CORSMiddleware; rebuilt on push from PluginConfig apply handler."""

    def __init__(self, app: ASGIApp):
        global _cors_instance
        self.app = app
        self._inner: Optional[CORSMiddleware] = None
        # Build initial middleware with defaults.
        self._rebuild(SecurityPluginConfig())
        _cors_instance = self

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
        logger.debug("DynamicCORSMiddleware: rebuilt with origins=%s", cfg.cors_allowed_origins)

    async def initialize_from_db(self) -> None:
        """One-time startup read from PlatformConfigsProtocol."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

            svc = get_protocol(PlatformConfigsProtocol)
            if svc is None:
                return
            cfg = await svc.get_config(SECURITY_PLUGIN_CONFIG_ID)
            if not isinstance(cfg, SecurityPluginConfig):
                cfg = SecurityPluginConfig.model_validate(
                    cfg.model_dump() if hasattr(cfg, "model_dump") else {}
                )
            self._rebuild(cfg)
            logger.info("DynamicCORSMiddleware: initialized from database")
        except Exception:
            logger.debug("DynamicCORSMiddleware: init from DB skipped (service not ready)")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._inner(scope, receive, send)  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
#  Push callback -- registered with PluginConfig apply handler
# ---------------------------------------------------------------------------


def on_security_config_changed(
    config: "SecurityPluginConfig",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[object],
) -> None:
    """Called by PluginConfig apply handler when security config is written."""
    if _cors_instance is None:
        return
    if not isinstance(config, SecurityPluginConfig):
        try:
            config = SecurityPluginConfig.model_validate(
                config.model_dump() if hasattr(config, "model_dump") else {}
            )
        except Exception:
            return
    _cors_instance._rebuild(config)
    logger.info("DynamicCORSMiddleware: CORS config pushed, middleware rebuilt")
