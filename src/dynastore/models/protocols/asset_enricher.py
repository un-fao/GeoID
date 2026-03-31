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
AssetEnricherProtocol — optional pipeline for enriching asset documents at
read time.

Enrichers are registered via ``register_plugin(enricher)`` and discovered
with ``get_protocols(AssetEnricherProtocol)``.  ``AssetService.get_asset()``
and ``AssetService.list_assets()`` run all active enrichers in ascending
``priority`` order, passing the asset doc through each stage.

Typical use cases:
  - Resolve a GCS/S3 URI to a signed URL and inject ``resolved_uri``.
  - Add per-asset statistics (download count, last access time) from an
    analytics driver (BigQuery, DuckDB).
  - Filter out asset fields based on the requesting user's role.
  - Pre-populate ``AssetContext`` fields needed by ``asset_factory.py``
    so that dynamic asset construction requires no additional DB round-trips.
"""

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class AssetEnricherProtocol(Protocol):
    """Pipeline stage for enriching or filtering asset documents at read time.

    Implementations are discovered via the plugin registry.  Each enricher
    declares a ``priority`` (lower = earlier) and a ``can_enrich()`` guard
    that controls whether the enricher applies to a given catalog/collection.

    The ``enrich_asset()`` method receives the asset document dict and returns
    a (possibly modified) copy.  Enrichers must not mutate the input dict in
    place.

    Attributes:
        enricher_id: Unique string identifier (e.g. ``"asset_uri_enricher"``).
        priority:    Execution order — lower runs first.  Use 100 for
                     general enrichers; < 100 for guards/filters that
                     should run before data is added.
    """

    enricher_id: str
    priority: int

    def can_enrich(
        self,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> bool:
        """Return ``True`` if this enricher applies to the given context.

        Called before ``enrich_asset()`` to skip enrichers that are not
        relevant (e.g. enrichers tied to a specific catalog or driver type).

        Args:
            catalog_id:    The catalog that owns the asset.
            collection_id: The collection the asset belongs to (may be None
                           for catalog-level assets).

        Returns:
            ``True`` to run ``enrich_asset()``; ``False`` to skip.
        """
        ...

    async def enrich_asset(
        self,
        catalog_id: str,
        asset_doc: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich or filter an asset document.

        Receives the current accumulated asset dict and returns a (possibly
        modified) dict.  Must not mutate ``asset_doc`` in place — return a
        new dict instead (e.g. ``{**asset_doc, "resolved_uri": url}``).

        Args:
            catalog_id: The catalog that owns the asset.
            asset_doc:  Current accumulated asset dict from previous stages.
            context:    Request-scoped context (e.g. authenticated user,
                        request headers, collection_id).  May be empty.

        Returns:
            Modified asset dict (or the original if no changes needed).
        """
        ...
