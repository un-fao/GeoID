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
BigQueryCollectionEnricher — adds BQ-sourced statistics to collection metadata.

Implements ``CollectionMetadataEnricherProtocol``.  When a collection has a
``bq_stats`` config (via ``ConfigsProtocol``), this enricher queries BigQuery
for row counts, last-modified timestamps, or other aggregate stats and merges
them into the collection metadata dict.

Registered via ``register_plugin()`` during GCPModule lifespan.
"""

import logging
from typing import Any, Dict

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

BQ_STATS_CONFIG_ID = "bq_stats"


class BigQueryCollectionEnricher:
    """CollectionMetadataEnricherProtocol implementation using BigQuery.

    Runs at priority 200 (after core enrichers) and only activates when
    a ``bq_stats`` config exists for the collection.
    """

    enricher_id: str = "bq_stats_enricher"
    priority: int = 200

    def can_enrich(self, catalog_id: str, collection_id: str) -> bool:
        """Always returns True; actual config check happens in ``enrich()``.

        The config lookup is async, so the guard is permissive and the
        enricher no-ops when no config is found.
        """
        return True

    async def enrich(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add BQ-sourced stats to collection metadata if configured.

        Looks up a ``bq_stats`` config for the collection.  If absent,
        returns metadata unchanged.  If present, runs the configured
        query and merges results under ``metadata["bq_stats"]``.
        """
        from dynastore.models.protocols import ConfigsProtocol, BigQueryProtocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return metadata

        try:
            stats_config = await configs.get_config(
                BQ_STATS_CONFIG_ID,  # type: ignore[arg-type]
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return metadata  # no config → nothing to enrich

        if not stats_config:
            return metadata

        bq = get_protocol(BigQueryProtocol)
        if not bq:
            logger.debug(
                "BigQueryCollectionEnricher: BigQueryProtocol not available for %s/%s",
                catalog_id, collection_id,
            )
            return metadata

        query = getattr(stats_config, "query", None)
        project_id = getattr(stats_config, "project_id", None)
        if not query or not project_id:
            return metadata

        try:
            records = await bq.execute_query(query, project_id)
            if records:
                return {**metadata, "bq_stats": records[0]}
        except Exception as exc:
            logger.warning(
                "BigQueryCollectionEnricher query failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )

        return metadata
