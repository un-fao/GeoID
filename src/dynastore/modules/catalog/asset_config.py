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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Asset storage routing configuration.

``AssetPluginConfig`` controls which driver receives asset writes, which
driver handles each read hint, and which drivers receive async write fan-out.
The 4-tier config waterfall (collection > catalog > platform > defaults)
applies, identical to ``CollectionPluginConfig``.

Default (no config) = ``write_driver="postgresql"`` — PG-only, identical to
current behaviour.

Supported read hints:
  ``"default"``  — general asset reads (falls back to write_driver)
  ``"search"``   — full-text / attribute search (auto-selects ES via preferred_for)
  ``"metadata"`` — collection-level asset metadata (counts, coverage, timestamps)
"""

import logging
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field, model_validator

from dynastore.modules.db_config.platform_config_service import (
    ConfigRegistry,
    Immutable,
    PluginConfig,
)
from dynastore.modules.storage.config import DriverRef

logger = logging.getLogger(__name__)

ASSET_PLUGIN_CONFIG_ID = "asset"


class AssetPluginConfig(PluginConfig):
    """Per-collection asset storage routing config.

    Mirrors ``CollectionPluginConfig`` but scoped to asset storage.
    Omit all fields for default PG-only behaviour.

    Examples::

        # Default — PG for everything (backward-compatible):
        AssetPluginConfig()

        # ES as primary write + read store:
        AssetPluginConfig(write_driver="elasticsearch_assets")

        # PG write + ES search + PG metadata:
        AssetPluginConfig(
            write_driver="postgresql",
            read_drivers={
                "search": "elasticsearch_assets",
                "metadata": "postgresql",
            },
            secondary_drivers=["elasticsearch_assets"],
        )
    """

    _plugin_id: ClassVar[Optional[str]] = ASSET_PLUGIN_CONFIG_ID

    model_config = {"extra": "allow"}

    write_driver: Immutable[DriverRef] = Field(
        default_factory=lambda: DriverRef(driver_id="postgresql"),
        description=(
            "Driver for writes.  Also serves as fallback for reads when no hint "
            "matches.  Immutable once assets exist for this collection."
        ),
    )
    read_drivers: Dict[str, DriverRef] = Field(
        default_factory=dict,
        description=(
            "Hint → driver mapping for reads.  "
            "Special hints: 'default' (fallback), 'search' (fulltext/attribute), "
            "'metadata' (collection-level asset stats).  "
            "If empty, write_driver handles all reads."
        ),
    )
    secondary_drivers: List[DriverRef] = Field(
        default_factory=list,
        description="Drivers receiving async write fan-out on every asset write.",
    )

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def write_driver_id(self) -> str:
        """Shortcut: write driver ID as string."""
        return self.write_driver.driver_id

    def resolve_read_driver_id(self, hint: str) -> str:
        """Resolve driver ID for a read hint: hint → default → write_driver."""
        ref = self.read_drivers.get(hint) or self.read_drivers.get("default")
        return ref.driver_id if ref else self.write_driver_id

    @property
    def secondary_driver_ids(self) -> List[str]:
        """Shortcut: secondary driver IDs as strings."""
        return [d.driver_id for d in self.secondary_drivers]

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def _coerce_driver_refs(cls, data: Any) -> Any:
        """Allow shorthand strings: ``'postgresql'`` → ``DriverRef(driver_id='postgresql')``."""
        if not isinstance(data, dict):
            return data

        wd = data.get("write_driver")
        if isinstance(wd, str):
            data["write_driver"] = {"driver_id": wd}

        rd = data.get("read_drivers")
        if isinstance(rd, dict):
            data["read_drivers"] = {
                k: {"driver_id": v} if isinstance(v, str) else v
                for k, v in rd.items()
            }

        sd = data.get("secondary_drivers")
        if isinstance(sd, list):
            data["secondary_drivers"] = [
                {"driver_id": s} if isinstance(s, str) else s for s in sd
            ]

        return data


# ------------------------------------------------------------------
# on_apply handler
# ------------------------------------------------------------------


async def _on_apply_asset_config(
    config: AssetPluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after ``AssetPluginConfig`` is written.

    Verifies newly-added secondary drivers are registered and performs
    best-effort cleanup for removed secondary drivers.  Also invalidates
    the asset router cache so the new config is picked up immediately.
    """
    from dynastore.models.protocols.asset_driver import AssetDriverProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol, get_protocols

    # Resolve previous config to compute diff
    old_config: Optional[AssetPluginConfig] = None
    if catalog_id:
        try:
            configs_proto = get_protocol(ConfigsProtocol)
            if configs_proto:
                old_config = await configs_proto.get_config(
                    ASSET_PLUGIN_CONFIG_ID,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
        except Exception:
            pass

    old_ids = set(old_config.secondary_driver_ids) if old_config else set()
    new_ids = set(config.secondary_driver_ids)

    added = new_ids - old_ids
    removed = old_ids - new_ids

    if added:
        registered = {d.driver_id for d in get_protocols(AssetDriverProtocol)}
        unknown = added - registered
        if unknown:
            logger.warning(
                "AssetConfig: secondary driver(s) not registered: %s. "
                "Available: %s. Writes will be silently ignored until the driver loads.",
                sorted(unknown),
                sorted(registered),
            )

    if removed and catalog_id:
        driver_index = {d.driver_id: d for d in get_protocols(AssetDriverProtocol)}
        for driver_id in removed:
            driver = driver_index.get(driver_id)
            if driver and hasattr(driver, "drop_storage"):
                try:
                    await driver.drop_storage(catalog_id, collection_id)
                    logger.info(
                        "AssetConfig: cleaned up driver '%s' for catalog '%s'.",
                        driver_id,
                        catalog_id,
                    )
                except Exception as e:
                    logger.warning(
                        "AssetConfig: cleanup failed for driver '%s' catalog '%s': %s",
                        driver_id,
                        catalog_id,
                        e,
                    )

    # Invalidate cached router resolution for this collection
    try:
        from dynastore.modules.catalog.asset_router import invalidate_asset_router_cache

        invalidate_asset_router_cache(catalog_id, collection_id)
    except Exception:
        pass  # router may not be imported yet during startup


ConfigRegistry.register_apply_handler(ASSET_PLUGIN_CONFIG_ID, _on_apply_asset_config)
AssetPluginConfig.model_rebuild()
