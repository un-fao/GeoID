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
EntityTransformPipeline — metadata enrichment via the TRANSFORM operation chain.

Implements ``CollectionMetadataEnricherProtocol``.  When a collection has
``metadata.operations[TRANSFORM]`` drivers configured in
``CollectionRoutingConfig``, this pipeline calls
``driver.get_collection_metadata()`` on each in declared order and merges
supplementary fields (summaries, providers, item_assets, assets,
extra_metadata) into the collection metadata dict.

If any configured storage driver declares ``Capability.AGGREGATION``, the
pipeline additionally calls ``driver.compute_extents()`` to fill
spatial/temporal extent when not already present.

Registered via ``register_plugin()`` during ``CatalogModule`` lifespan.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EntityTransformPipeline:
    """CollectionMetadataEnricherProtocol implementation using the TRANSFORM chain.

    Runs at priority 200 (after core enrichers).  Activates when
    ``metadata.operations[TRANSFORM]`` entries are configured for the collection.
    """

    enricher_id: str = "entity_transform_pipeline"
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

        Reads ``CollectionRoutingConfig.metadata.operations[TRANSFORM]`` to find
        configured storage drivers, then calls ``get_collection_metadata()`` on each
        in declared order, supplementing (not overwriting) existing fields.

        If any driver supports ``Capability.AGGREGATION`` and the metadata
        has no ``extent``, also calls ``compute_extents()``.
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionItemsStore,
        )
        from dynastore.modules.storage.routing_config import CollectionRoutingConfig, Operation
        from dynastore.tools.discovery import get_protocol, get_protocols

        # Resolve metadata.operations[TRANSFORM] entries from the routing config
        try:
            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return metadata
            routing_config = await configs.get_config(
                CollectionRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            transform_entries = routing_config.metadata.operations.get(
                Operation.TRANSFORM, []
            )
        except Exception:
            return metadata

        if not transform_entries:
            return metadata

        driver_index = {
            type(d).__name__: d for d in get_protocols(CollectionItemsStore)
        }
        merged = {**metadata}

        for entry in transform_entries:
            driver = driver_index.get(entry.driver_id)
            if not driver:
                logger.debug(
                    "EntityTransformPipeline: TRANSFORM driver '%s' not registered",
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
                    "EntityTransformPipeline: get_collection_metadata failed for "
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
                        "EntityTransformPipeline: compute_extents failed for "
                        "driver '%s' on %s/%s: %s",
                        entry.driver_id, catalog_id, collection_id, exc,
                    )

        return merged
