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
DriverMetadataEnricher — bridges non-PG drivers into the enricher pipeline.

Implements ``CollectionMetadataEnricherProtocol``.  When a collection has
``metadata.storage`` drivers configured in ``RoutingPluginConfig``, this
enricher calls ``driver.get_collection_metadata()`` on each and merges
supplementary fields (summaries, providers, item_assets, assets,
extra_metadata) into the collection metadata dict.

If any configured storage driver declares ``Capability.AGGREGATION``, the
enricher additionally calls ``driver.compute_extents()`` to fill
spatial/temporal extent when not already present.

Registered via ``register_plugin()`` during ``CatalogModule`` lifespan.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class DriverMetadataEnricher:
    """CollectionMetadataEnricherProtocol implementation using storage drivers.

    Runs at priority 200 (after core enrichers).  Activates when a
    METADATA-routed driver is configured for the collection.
    """

    enricher_id: str = "driver_metadata"
    priority: int = 200

    def can_enrich(self, catalog_id: str, collection_id: str = "") -> bool:
        """Returns True only for collection-level enrichment (requires collection_id)."""
        return bool(collection_id)

    async def enrich(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Merge driver-sourced metadata into the collection metadata dict.

        Reads ``RoutingPluginConfig.metadata.storage`` to find configured
        storage drivers, then calls ``get_collection_metadata()`` on each,
        supplementing (not overwriting) existing fields in ``metadata``.

        If any driver supports ``Capability.AGGREGATION`` and the metadata
        has no ``extent``, also calls ``compute_extents()``.
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionStorageDriverProtocol,
        )
        from dynastore.modules.storage.routing_config import ROUTING_PLUGIN_CONFIG_ID
        from dynastore.tools.discovery import get_protocol, get_protocols

        # Resolve metadata.storage entries from the routing config
        try:
            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return metadata
            routing_config = await configs.get_config(
                ROUTING_PLUGIN_CONFIG_ID,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            storage_entries = routing_config.metadata.storage
        except Exception:
            return metadata

        if not storage_entries:
            return metadata

        driver_index = {
            d.driver_id: d for d in get_protocols(CollectionStorageDriverProtocol)
        }
        merged = {**metadata}

        for entry in storage_entries:
            driver = driver_index.get(entry.driver_id)
            if not driver:
                logger.debug(
                    "DriverMetadataEnricher: metadata.storage driver '%s' not registered",
                    entry.driver_id,
                )
                continue

            # Fetch driver-managed metadata (supplement only — never overwrite)
            try:
                driver_meta = await driver.get_collection_metadata(
                    catalog_id, collection_id,
                )
            except Exception as exc:
                logger.debug(
                    "DriverMetadataEnricher: get_collection_metadata failed for "
                    "driver '%s' on %s/%s: %s",
                    entry.driver_id, catalog_id, collection_id, exc,
                )
                continue

            if not driver_meta:
                continue

            for key in (
                "summaries", "providers", "item_assets", "assets",
                "extra_metadata", "links",
            ):
                if key in driver_meta and key not in merged:
                    merged[key] = driver_meta[key]

            # Extents from driver if it supports AGGREGATION and no extent yet
            if (
                "extent" not in merged
                and Capability.AGGREGATION in getattr(driver, "capabilities", frozenset())
            ):
                try:
                    extents = await driver.compute_extents(catalog_id, collection_id)
                    if extents:
                        merged["extent"] = extents
                except Exception as exc:
                    logger.debug(
                        "DriverMetadataEnricher: compute_extents failed for "
                        "driver '%s' on %s/%s: %s",
                        entry.driver_id, catalog_id, collection_id, exc,
                    )

        return merged
