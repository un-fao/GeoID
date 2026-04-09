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
ItemEnricherProtocol — optional pipeline for enriching or filtering
individual items/features at read time.

Enrichers are registered via ``register_plugin(enricher)`` and discovered
with ``get_protocols(ItemEnricherProtocol)``.  The ``_apply_item_enrichers()``
helper in ``item_query.py`` wraps the item stream with an async generator
that applies all active enrichers to each feature, preserving streaming
semantics (items are never collected into a list).

The ``can_enrich()`` guard is evaluated **once per query** (not per item)
to avoid per-item overhead.

Typical use cases:
  - Join DWH statistics (e.g. download count) from BigQuery per feature.
  - Add signed asset URLs resolved from GCS/S3.
  - Filter out sensitive properties based on user role.
  - Inject computed fields (e.g. derived temporal aggregates).
"""

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class ItemEnricherProtocol(Protocol):
    """Pipeline stage for enriching or filtering individual items/features.

    Implementations are discovered via the plugin registry.  Each enricher
    declares a ``priority`` (lower = earlier) and a ``can_enrich()`` guard
    that controls whether the enricher applies to a given collection.

    The ``enrich_item()`` method receives a single feature dict and returns
    a (possibly modified) copy.  Enrichers must not mutate the input dict
    in place.

    Attributes:
        enricher_id: Unique string identifier (e.g. ``"bq_item_stats"``).
        priority:    Execution order — lower runs first.  Use 100 for
                     general enrichers; < 100 for guards/filters.
    """

    enricher_id: str
    priority: int

    def can_enrich(self, catalog_id: str, collection_id: str) -> bool:
        """Return ``True`` if this enricher applies to the given collection.

        Called **once per query** before the item stream is iterated.

        Args:
            catalog_id:    The catalog that owns the collection.
            collection_id: The collection whose items are being read.

        Returns:
            ``True`` to run ``enrich_item()`` on each feature;
            ``False`` to skip this enricher entirely.
        """
        ...

    async def enrich_item(
        self,
        catalog_id: str,
        collection_id: str,
        feature: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich or filter a single feature/item.

        Receives the current feature dict and returns a (possibly modified)
        dict.  Must not mutate ``feature`` in place — return a new dict
        instead (e.g. ``{**feature, "extra_key": value}``).

        Args:
            catalog_id:    The catalog that owns the collection.
            collection_id: The collection the item belongs to.
            feature:       Current feature dict from previous pipeline stages.
            context:       Request-scoped context (e.g. authenticated user,
                           request headers).  May be empty.

        Returns:
            Modified feature dict (or the original if no changes needed).
        """
        ...
