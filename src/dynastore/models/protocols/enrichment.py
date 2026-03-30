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
CollectionMetadataEnricherProtocol — optional pipeline for enriching or
filtering collection-level metadata at read time.

Enrichers are registered via ``register_plugin(enricher)`` and discovered
with ``get_protocols(CollectionMetadataEnricherProtocol)``.  The pipeline in
``_get_collection_model_logic()`` runs all enrichers in ascending
``priority`` order, passing the accumulated metadata dict through each.

Typical use cases:
  - Add real-time row counts from BigQuery / Iceberg snapshot info.
  - Filter out metadata fields based on the requesting user's role.
  - Inject computed summaries (e.g. temporal extent from event log).
"""

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class CollectionMetadataEnricherProtocol(Protocol):
    """Pipeline stage for enriching or filtering collection metadata.

    Implementations are discovered via the plugin registry.  Each enricher
    declares a ``priority`` (lower = earlier) and a ``can_enrich()`` guard
    that controls whether the enricher applies to a given collection.

    The ``enrich()`` method receives the accumulated metadata dict and
    returns a (possibly modified) copy.  Enrichers must not mutate the
    input dict in place.

    Attributes:
        enricher_id: Unique string identifier (e.g. ``"bq_stats_enricher"``).
        priority:    Execution order — lower runs first.  Use 100 for
                     general enrichers; < 100 for guards/filters that
                     should run before data is added.
    """

    enricher_id: str
    priority: int

    def can_enrich(self, catalog_id: str, collection_id: str) -> bool:
        """Return ``True`` if this enricher applies to the given collection.

        Called before ``enrich()`` to skip enrichers that are not relevant
        (e.g. enrichers tied to a specific catalog or driver type).

        Args:
            catalog_id:    The catalog that owns the collection.
            collection_id: The collection whose metadata is being read.

        Returns:
            ``True`` to run ``enrich()``; ``False`` to skip this enricher.
        """
        ...

    async def enrich(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich or filter collection metadata.

        Receives the current accumulated metadata dict and returns a
        (possibly modified) dict.  Must not mutate ``metadata`` in place —
        return a new dict instead (e.g. ``{**metadata, "extra_key": value}``).

        Args:
            catalog_id:    The catalog that owns the collection.
            collection_id: The collection being enriched.
            metadata:      Current accumulated metadata from previous stages.
            context:       Request-scoped context (e.g. authenticated user,
                           request headers).  May be empty.

        Returns:
            Modified metadata dict (or the original if no changes needed).
        """
        ...
