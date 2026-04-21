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
EntityTransformPipeline — TRANSFORM-chain envelope merger (library code).

**Status**: no call sites today.  ``enrich()`` is preserved for M3's
``ReindexWorker`` to invoke directly when it hydrates transformed
envelopes for INDEX / BACKUP propagation.

What ``enrich()`` does today: reads
``CollectionRoutingConfig.metadata.operations[TRANSFORM]`` to find
configured ``CollectionItemsStore`` drivers and, for any driver
declaring ``Capability.AGGREGATION``, invokes ``compute_extents()`` to
fill in a missing ``extent`` field.

Historical note: this pipeline used to also fan out
``driver.get_collection_metadata()`` across TRANSFORM drivers.  That
protocol surface was removed in M2.5 — collection metadata now lives
exclusively on ``CollectionMetadataStore`` drivers and is read via
``collection_metadata_router.get_collection_metadata()``.  M3 will
decide whether the TRANSFORM merge still makes sense as a concept or
whether the router slice-merge is enough on its own.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EntityTransformPipeline:
    """Library helper — merges driver-sourced extents into a metadata envelope.

    Instantiate and call :meth:`enrich` directly.  Not registered as a
    plugin; no protocol discovery.  M3's ``ReindexWorker`` is the
    intended caller.
    """

    async def enrich(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Supplement the metadata envelope with driver-computed extents.

        Reads ``CollectionRoutingConfig.metadata.operations[TRANSFORM]``
        to find configured storage drivers.  For each driver that
        declares ``Capability.AGGREGATION``, calls ``compute_extents()``
        and fills ``extent`` when absent from the envelope.  Never
        overwrites existing fields.
        """
        if not collection_id:
            return metadata

        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionItemsStore,
        )
        from dynastore.modules.storage.routing_config import (
            CollectionRoutingConfig,
            Operation,
        )
        from dynastore.tools.discovery import get_protocol, get_protocols

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

            if (
                "extent" not in merged
                and Capability.AGGREGATION
                in getattr(driver, "capabilities", frozenset())
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
