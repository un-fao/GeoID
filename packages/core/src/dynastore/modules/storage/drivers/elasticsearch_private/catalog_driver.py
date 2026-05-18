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

"""``CatalogElasticsearchPrivateDriver`` — catalog-envelope tenant-scoped ES driver.

#960: parallels :class:`CollectionElasticsearchPrivateDriver` at the
catalog-envelope tier.  Stores the catalog document in a per-tenant
private index ``{prefix}-{catalog_id}-catalog-private`` instead of the
shared platform-wide ``{prefix}-catalogs``.

Privacy semantics
-----------------
- Privacy is expressed by pinning this driver in
  :class:`CatalogRoutingConfig` (no ``is_private`` flag — same pattern
  as the items + collection tiers under #733).
- DENY-policy ownership stays with the items-tier private driver: the
  catalog-wide ``private_deny_{catalog_id}`` URL pattern
  (``/.../catalogs/{cat}(/.*)?``) already covers both the catalog-envelope
  endpoint AND every nested resource.  No separate DENY needed here.
- Opts out of auto-default routing
  (``auto_register_for_routing = frozenset()``); operators pin it
  explicitly in :class:`CatalogRoutingConfig` for catalogs whose
  envelope must not appear in the shared platform index.

Index design
------------
- Per-catalog index — single doc (the catalog itself), keyed by
  ``catalog_id``.
- Same ``CATALOG_MAPPING`` as the public driver (privacy is enforced at
  the routing/IAM layer, not via mapping reduction).

Wiring
------
- Registered as ``catalog_elasticsearch_private`` via ``pyproject.toml``
  entry point (parallel to ``collection_elasticsearch_private`` for the
  collection tier and ``storage_elasticsearch_private`` for items).
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, FrozenSet, Optional

from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriver,
)
from dynastore.modules.storage.storage_location import StorageLocation

logger = logging.getLogger(__name__)


class CatalogElasticsearchPrivateDriver(CatalogElasticsearchDriver):
    """Tenant-scoped Elasticsearch driver for catalog envelopes.

    Subclass of the public :class:`CatalogElasticsearchDriver` —
    inherits the full ``CatalogStore`` + ``CatalogIndexer`` surface and
    overrides the methods that resolve the index name, swapping the
    shared ``{prefix}-catalogs`` for the per-tenant
    ``{prefix}-{catalog_id}-catalog-private``.
    """

    # Opt out of auto-default routing.  See module docstring.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    # Extend the parent's capabilities with TENANT_ISOLATED so the privacy
    # cascade can find per-tenant catalog-tier drivers via capability
    # membership instead of isinstance() on the concrete class.
    capabilities: FrozenSet[str] = frozenset(
        CatalogElasticsearchDriver.capabilities
        | {EntityStoreCapability.TENANT_ISOLATED}
    )

    # ------------------------------------------------------------------
    # Index-name resolution — the only structural difference from the
    # public driver.  Every catalog_id-aware method below computes the
    # per-tenant index name and forwards to the same ES client surface.
    # ------------------------------------------------------------------

    def _private_index(self, catalog_id: str) -> str:
        from dynastore.modules.elasticsearch.mappings import (
            get_tenant_catalog_private_index,
        )

        return get_tenant_catalog_private_index(self._get_prefix(), catalog_id)

    # ------------------------------------------------------------------
    # CatalogStore surface — overrides
    # ------------------------------------------------------------------

    def location(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> StorageLocation:
        prefix = self._get_prefix()
        index = self._private_index(catalog_id)
        return StorageLocation(
            backend="elasticsearch_private",
            canonical_uri=f"es://{index}",
            identifiers={
                "index": index,
                "prefix": prefix,
                "catalog_id": catalog_id,
            },
            display_label=index,
        )

    async def ensure_storage(self, catalog_id: Optional[str] = None) -> None:
        """Create the per-tenant private catalog index with the public
        catalog mapping (same shape — privacy is enforced at the
        routing/IAM layer, not via mapping reduction).

        Unlike the public driver — whose shared index lives at platform
        scope and accepts ``catalog_id=None`` — this driver requires a
        ``catalog_id`` because its index name is per-tenant.  No-op when
        ``catalog_id`` is missing.
        """
        if not catalog_id:
            return
        client = self._get_client()
        if client is None:
            return
        index_name = self._private_index(catalog_id)
        from dynastore.modules.elasticsearch.mappings import CATALOG_MAPPING

        try:
            exists = await client.indices.exists(index=index_name)
            if exists:
                return
            try:
                await client.indices.create(
                    index=index_name,
                    body={
                        "mappings": CATALOG_MAPPING,
                        "settings": {
                            "number_of_shards": 1,
                            "number_of_replicas": 0,
                        },
                    },
                )
            except Exception as exc:
                # Tolerate "already exists" races between concurrent
                # ensure_storage callers. opensearch-py 3.x dropped the
                # ``ignore=`` kwarg, so we catch and filter instead.
                if "resource_already_exists" not in str(exc):
                    raise
            logger.info(
                "CatalogElasticsearchPrivateDriver: created %r.", index_name,
            )
        except Exception as exc:
            logger.warning(
                "CatalogElasticsearchPrivateDriver: ensure_storage(%r) failed: %s",
                catalog_id, exc,
            )

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None
        index_name = self._private_index(catalog_id)
        try:
            resp = await client.get(index=index_name, id=catalog_id)
            return resp["_source"]
        except Exception:
            return None

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")
        # Lazy index creation — mirrors the collection-private driver's
        # defensive pattern: until the catalog ensure_storage lifecycle
        # hook reliably fires before the first upsert, self-create.
        await self.ensure_storage(catalog_id)
        index_name = self._private_index(catalog_id)
        doc = dict(metadata)
        doc["id"] = catalog_id
        doc["catalog_id"] = catalog_id
        try:
            await client.index(
                index=index_name,
                id=catalog_id,
                body=doc,
                params={"refresh": "wait_for"},
            )
        except Exception:
            logger.warning(
                "CatalogElasticsearchPrivateDriver.upsert_catalog_metadata failed: "
                "catalog=%r index=%r",
                catalog_id, index_name,
                exc_info=True,
            )
            raise

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            return
        index_name = self._private_index(catalog_id)
        try:
            if soft:
                await client.update(
                    index=index_name,
                    id=catalog_id,
                    body={"doc": {"_deleted": True}},
                    params={"refresh": "wait_for"},
                )
            else:
                await client.delete(
                    index=index_name,
                    id=catalog_id,
                    params={"refresh": "wait_for"},
                )
        except Exception as exc:
            logger.debug(
                "CatalogElasticsearchPrivateDriver.delete_catalog_metadata(%s) failed: %s",
                catalog_id, exc,
            )

    async def drop_storage(self, catalog_id: str, *, soft: bool = False) -> None:
        """Delete the per-tenant private catalog index.  Idempotent —
        missing index is treated as a no-op.
        """
        client = self._get_client()
        if client is None:
            return
        index_name = self._private_index(catalog_id)
        try:
            try:
                await client.indices.delete(index=index_name)
            except Exception as exc:
                if "index_not_found" not in str(exc) and "not_found" not in str(exc):
                    raise
            logger.info(
                "CatalogElasticsearchPrivateDriver: dropped %r.", index_name,
            )
        except Exception as exc:
            logger.warning(
                "CatalogElasticsearchPrivateDriver: drop_storage(%r) failed: %s",
                catalog_id, exc,
            )

    # ------------------------------------------------------------------
    # Indexer Protocol — override to plumb catalog_id through to
    # ensure_storage (the parent's ensure_indexer ignores ctx.catalog).
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx: Any) -> None:
        await self.ensure_storage(getattr(ctx, "catalog", None))

    # NOTE: ``index`` + ``index_bulk`` inherit unchanged from the parent.
    # They call ``upsert_catalog_metadata`` / ``delete_catalog_metadata``,
    # which we've overridden to write to the per-tenant index — so the
    # bulk indexer paths route correctly without further overrides.
