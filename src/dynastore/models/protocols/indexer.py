#    Copyright 2026 FAO
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
Indexer protocol definitions.

Abstracts document indexing lifecycle so that event-driven modules can
dispatch indexing operations without coupling to a specific search backend.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Protocol, runtime_checkable


@runtime_checkable
class IndexerProtocol(Protocol):
    """
    Protocol for document indexing operations.

    Implementations manage the lifecycle of indexed documents —
    create/update, delete, and bulk reindex — without exposing backend
    specifics.  The event-driven module (``ElasticsearchModule``) is the
    primary consumer; it dispatches indexing tasks via this protocol.

    Implementors:
        - ``ElasticsearchModule`` in ``modules/elasticsearch/module.py``
          (dispatches tasks to the Elasticsearch cluster).
    """

    async def index_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        document: Dict[str, Any],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Index or update a single document.

        Args:
            entity_type: The entity kind being indexed.
            entity_id: Unique document identifier.
            document: Full document payload to index.
            catalog_id: Owning catalog (used for index routing/naming).
            collection_id: Owning collection (optional).
            db_resource: Database resource for transactional context.
        """
        ...

    async def delete_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        catalog_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Remove a document from the search index.

        Args:
            entity_type: The entity kind being deleted.
            entity_id: The document ID to delete.
            catalog_id: Owning catalog.
            db_resource: Database resource for transactional context.
        """
        ...

    async def index_obfuscated(
        self,
        geoid: str,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Index a single geoid-only document in the obfuscated index.

        This is the privacy-preserving counterpart of ``index_document``.
        The indexed document contains only ``{geoid, catalog_id,
        collection_id}`` — no geometry, no STAC metadata.

        Args:
            geoid: The obfuscated item identifier.
            catalog_id: Owning catalog.
            collection_id: Owning collection.
            db_resource: Database resource for transactional context.
        """
        ...

    async def delete_obfuscated(
        self,
        geoid: str,
        catalog_id: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Remove a single geoid document from the obfuscated index.

        Safe to call even if the document does not exist (no-op).

        Args:
            geoid: The obfuscated item identifier.
            catalog_id: Owning catalog.
            db_resource: Database resource for transactional context.
        """
        ...

    async def bulk_reindex(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        mode: Literal["catalog", "obfuscated"] = "catalog",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a bulk reindex of all items in a catalog or collection.

        This is typically dispatched as a durable background task or
        Cloud Run Job rather than executed inline.

        Args:
            catalog_id: The catalog to reindex.
            collection_id: Optional single collection (if ``None``, all
                           collections in the catalog are reindexed).
            mode: ``"catalog"`` (full catalog documents) or ``"obfuscated"``
                  (geoid-only documents).
            db_resource: Database resource for transactional context.

        Returns:
            Dict with reindex result metadata (``total_indexed``, ``status``).
        """
        ...

    async def ensure_index(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset", "obfuscated"],
        catalog_id: Optional[str] = None,
    ) -> None:
        """
        Ensure the index for the given entity type exists, creating it
        with the correct mapping if necessary.

        Args:
            entity_type: The index to ensure. Use ``"obfuscated"`` for
                         the per-catalog geoid-only index.
            catalog_id: Required when ``entity_type="obfuscated"`` (index
                        is per-catalog).
        """
        ...
