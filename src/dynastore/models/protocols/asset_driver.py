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
Asset storage driver protocol.

Drivers that manage asset-level storage (index, search, delete asset documents)
implement this protocol.  They differ from ``CollectionStorageDriverProtocol``
which deals with geographic features — asset drivers deal with metadata about
files, services, or datasets attached to a collection.

A driver may implement both protocols if it handles both features and assets
(unlikely in practice; separate drivers are preferred).
"""

from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)


@runtime_checkable
class AssetDriverProtocol(Protocol):
    """Protocol for drivers that store and search asset metadata.

    Implementations:
    - ``DriverAssetElasticsearch`` — asset metadata in per-catalog ES index
    """

    capabilities: FrozenSet[str]
    preferred_for: FrozenSet[str]
    supported_hints: FrozenSet[str]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """Create or verify the backing storage for asset documents.

        Called during collection creation or driver activation.
        """
        ...

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """Remove the backing storage for asset documents."""
        ...

    # ------------------------------------------------------------------
    # Asset CRUD
    # ------------------------------------------------------------------

    async def index_asset(
        self,
        catalog_id: str,
        asset_doc: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Index (upsert) a single asset document."""
        ...

    async def delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete a single asset document by ID."""
        ...

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a single asset document by ID, or None if not found.

        Required for any driver that can serve as the primary readable store
        (e.g. Elasticsearch as primary, PostgreSQL as primary).
        """
        ...

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        query: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """Return asset documents matching the query.

        Args:
            catalog_id:    Catalog that owns the assets.
            collection_id: Optional collection filter.
            query:         Driver-specific query dict (ES query DSL, SQL filters, etc.).
            limit:         Maximum number of results.
            offset:        Skip this many results (for pagination).
            db_resource:   Optional existing DB connection/session.

        Returns:
            List of asset dicts (may be empty).
        """
        ...

    # ------------------------------------------------------------------
    # Collection metadata (separated from asset data — enables enrichment)
    #
    # These methods store collection-level descriptors (title, extent,
    # keywords, etc.) independently of the asset documents.  The
    # CollectionMetadataEnricherProtocol pipeline can therefore use
    # AssetDriverProtocol.get_collection_metadata() to enrich base
    # collection metadata with asset-derived information (e.g. asset
    # counts, ingestion timestamps, coverage).
    # ------------------------------------------------------------------

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return collection metadata stored by this driver.

        Returns None if no metadata has been stored.
        """
        ...

    async def set_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Store collection metadata in this driver's backing store."""
        ...
