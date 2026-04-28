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
ItemsElasticsearchPrivateDriver — tenant-scoped ES storage driver.

Writes the full feature (geometry + properties + external_id) into a
per-tenant index whose name is owned by this subpackage
(``{prefix}-geoid-{catalog_id}``). Driver-private mapping
(``TENANT_FEATURE_MAPPING``) lives in :mod:`.mappings`.

Manages DENY access policies in its own lifecycle. Reuses the shared
SFEOS-backed helpers from :class:`_ElasticsearchBase` (parent class
imported from the sibling regular driver module).

Registered as ``storage_elasticsearch_private`` via entry points.
"""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.drivers.elasticsearch import _ElasticsearchBase
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

logger = logging.getLogger(__name__)


class ItemsElasticsearchPrivateDriver(_ElasticsearchBase, ModuleProtocol):
    """Tenant-scoped Elasticsearch storage driver (a.k.a. "private").

    Writes the full feature (geometry + properties + external_id) into a
    per-tenant index ``{prefix}-geoid-{catalog_id}`` shared across all
    collections of the catalog. The mapping is `TENANT_FEATURE_MAPPING`
    (root ``dynamic: false`` to reject smuggled fields; ``properties.*``
    is dynamic so tenant attributes index without mapping churn).

    Docs that would exceed the ES 10MB per-doc limit are shrunk by
    `simplify_to_fit` (`tools/geometry_simplify.py`); the resulting
    `simplification_factor` and `simplification_mode` are persisted on
    the doc so clients can tell how much fidelity was lost.

    Search is gated by the tenant-first contract enforced in
    `extensions/search` (catalog_id required; geoid OR (external_id +
    collection_id) — never external_id alone).

    Manages DENY access policies in its own lifecycle.

    Uses the raw ES client from SFEOS settings (not DatabaseLogic) since
    the index has a custom mapping not managed by SFEOS.

    Registered as ``storage_elasticsearch_private`` via entry points.
    """

    priority: int = 51
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
    })
    preferred_for: FrozenSet[str] = frozenset()
    supported_hints: FrozenSet[str] = frozenset()

    def is_available(self) -> bool:
        return self._sfeos_available()

    def _get_client(self):
        """Get the async ES client from SFEOS DatabaseLogic."""
        return self._get_db_logic().client

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.catalog.event_service import CatalogEventType

        await self._restore_deny_policies()

        events = get_protocol(EventsProtocol)
        if events:
            for etype, handler in [
                (CatalogEventType.ITEM_CREATION, self._on_item_upsert),
                (CatalogEventType.ITEM_UPDATE, self._on_item_upsert),
                (CatalogEventType.ITEM_DELETION, self._on_item_delete),
                (CatalogEventType.ITEM_HARD_DELETION, self._on_item_delete),
                (CatalogEventType.BULK_ITEM_CREATION, self._on_item_bulk_upsert),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
            logger.info("ItemsElasticsearchPrivateDriver: event listeners registered.")
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        bulk_body: list = []
        for item in items:
            geoid = self._extract_item_id(item)
            if not geoid:
                continue
            doc = build_tenant_feature_doc(
                item, catalog_id=catalog_id, collection_id=collection_id,
            )
            doc, factor, mode = simplify_to_fit(doc)
            doc["simplification_factor"] = factor
            doc["simplification_mode"] = mode
            bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
            bulk_body.append(doc)

        if bulk_body:
            await es.bulk(body=bulk_body, request_timeout=60)

        return items if isinstance(items, list) else list(items)

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        if not entity_ids:
            return

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for geoid in entity_ids:
            try:
                resp = await es.get(index=index_name, id=geoid)
                source = resp["_source"]
                props = dict(source.get("properties") or {})
                # Surface tenant-feature bookkeeping in properties so callers
                # can detect simplification without a separate API.
                if "external_id" in source:
                    props["external_id"] = source["external_id"]
                if "simplification_factor" in source:
                    props["simplification_factor"] = source["simplification_factor"]
                if "simplification_mode" in source:
                    props["simplification_mode"] = source["simplification_mode"]
                props["catalog_id"] = source.get("catalog_id", catalog_id)
                props["collection_id"] = source.get("collection_id", collection_id)
                yield Feature(
                    type="Feature",
                    id=source.get("geoid", geoid),
                    geometry=source.get("geometry"),
                    properties=props,
                    bbox=source.get("bbox"),  # type: ignore[call-arg]
                )
            except Exception:
                pass

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchPrivateDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0

        for geoid in entity_ids:
            try:
                await es.delete(index=index_name, id=geoid)
                deleted += 1
            except Exception:
                pass

        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        await self._apply_deny_policy(catalog_id)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchPrivateDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        await es.indices.delete(index=index_name, ignore_unavailable=True)
        await self._revoke_deny_policy(catalog_id)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "ItemsElasticsearchPrivateDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_item_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        item_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        if await self._is_write_driver_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
            from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
                build_tenant_feature_doc,
            )
            from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
                TENANT_FEATURE_MAPPING,
                get_private_index_name,
            )
            from dynastore.tools.geometry_simplify import simplify_to_fit

            index_name = get_private_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()

            if not await es.indices.exists(index=index_name):
                await es.indices.create(
                    index=index_name,
                    body={"mappings": TENANT_FEATURE_MAPPING},
                    ignore=400,
                )
            # Event payload may carry the full STAC item; fall back to a
            # geoid-only doc when no payload is available.
            src = payload if isinstance(payload, dict) else {"id": item_id}
            src.setdefault("id", item_id)
            doc = build_tenant_feature_doc(
                src, catalog_id=catalog_id, collection_id=collection_id,
            )
            doc, factor, mode = simplify_to_fit(doc)
            doc["simplification_factor"] = factor
            doc["simplification_mode"] = mode
            await es.index(index=index_name, id=item_id, document=doc)
        except Exception as e:
            logger.error(
                "PrivateDriver: index failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    async def _on_item_bulk_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        if await self._is_write_driver_for(type(self).__name__, catalog_id, collection_id):
            return

        items_subset = (payload if isinstance(payload, dict) else {}).get("items_subset", [])
        if not items_subset:
            return

        try:
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
            from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
                build_tenant_feature_doc,
            )
            from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
                TENANT_FEATURE_MAPPING,
                get_private_index_name,
            )
            from dynastore.tools.geometry_simplify import simplify_to_fit

            index_name = get_private_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()

            if not await es.indices.exists(index=index_name):
                await es.indices.create(
                    index=index_name,
                    body={"mappings": TENANT_FEATURE_MAPPING},
                    ignore=400,
                )

            bulk_body: list = []
            for item_doc in items_subset:
                geoid = item_doc.get("id")
                if not geoid:
                    continue
                doc = build_tenant_feature_doc(
                    item_doc, catalog_id=catalog_id, collection_id=collection_id,
                )
                doc, factor, mode = simplify_to_fit(doc)
                doc["simplification_factor"] = factor
                doc["simplification_mode"] = mode
                bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
                bulk_body.append(doc)
            if bulk_body:
                await es.bulk(body=bulk_body, request_timeout=60)
        except Exception as e:
            logger.error(
                "PrivateDriver: bulk index failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_delete(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        item_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not item_id:
            _val = (payload if isinstance(payload, dict) else {}).get("geoid")
            item_id = str(_val) if _val is not None else None
        if not catalog_id or not item_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
            from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
                get_private_index_name,
            )

            index_name = get_private_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()
            try:
                await es.delete(index=index_name, id=item_id)
            except Exception:
                pass
        except Exception as e:
            logger.error("PrivateDriver: delete failed for %s: %s", item_id, e)

    # ------------------------------------------------------------------
    # DENY policy management (self-contained)
    # ------------------------------------------------------------------

    @staticmethod
    async def _apply_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        policy_id = f"private_deny_{catalog_id}"
        deny_policy = Policy(
            id=policy_id,
            description=f"Blocks public access to private catalog: {catalog_id}",
            actions=["GET"],
            resources=[
                f"/(catalog|stac|features|tiles|wfs|maps)/catalogs/{re.escape(catalog_id)}(/.*)?",
            ],
            effect="DENY",
        )

        perm.register_policy(deny_policy)
        perm.register_role(Role(name="all_users", policies=[policy_id]))

        try:
            await perm.create_policy(deny_policy)
        except Exception:
            try:
                await perm.update_policy(deny_policy)
            except Exception as e:
                logger.error("DENY policy persist failed for '%s': %s", catalog_id, e)

    @staticmethod
    async def _revoke_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        try:
            await perm.delete_policy(f"private_deny_{catalog_id}")
        except Exception:
            pass

    async def _restore_deny_policies(self) -> None:
        """Restore DENY policies at startup for catalogs using this driver."""
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.catalog.catalog_config import CollectionPluginConfig

            catalogs_proto = get_protocol(CatalogsProtocol)
            configs = get_protocol(ConfigsProtocol)
            if not catalogs_proto or not configs:
                return

            offset, batch = 0, 100
            while True:
                catalog_list = await catalogs_proto.list_catalogs(
                    limit=batch, offset=offset,
                )
                if not catalog_list:
                    break
                for catalog in catalog_list:
                    catalog_id = getattr(catalog, "id", None)
                    if not catalog_id:
                        continue
                    try:
                        routing = await configs.get_config(
                            CollectionPluginConfig, catalog_id=catalog_id,
                        )
                        if type(self).__name__ in routing.secondary_driver_ids:  # type: ignore[attr-defined]
                            await self._apply_deny_policy(catalog_id)
                            logger.info(
                                "PrivateDriver: restored DENY for '%s'.",
                                catalog_id,
                            )
                    except Exception:
                        continue
                if len(catalog_list) < batch:
                    break
                offset += batch
        except Exception as e:
            logger.warning(
                "PrivateDriver: could not restore DENY policies: %s", e,
            )

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this private index."""
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )
        from dynastore.modules.storage.storage_location import StorageLocation

        prefix = _get_index_prefix()
        index_name = get_private_index_name(prefix, catalog_id)
        return StorageLocation(
            backend="elasticsearch_private",
            canonical_uri=f"es://{index_name}",
            identifiers={"index": index_name, "prefix": prefix, "catalog_id": catalog_id},
            display_label=index_name,
        )
