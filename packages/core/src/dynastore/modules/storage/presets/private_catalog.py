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

"""``private_catalog`` preset — PG-only envelopes + items-tier private ES.

Private catalogs and collections are stored in PostgreSQL only — no ES
private index at catalog or collection envelope tier (#1047 Phase 2).
Items privacy is expressed by pinning ``items_elasticsearch_private_driver``
in the per-collection ``ItemsRoutingConfig``.

The items-private driver's ``private_deny_{catalog_id}`` policy gates
anonymous reads on the items URL patterns. No audience opt-ins.
"""
from __future__ import annotations

from typing import ClassVar, Any

from dynastore.modules.catalog.catalog_config import _build_private_items_routing

from .protocol import PresetBundle, PresetTier


def _build_pg_only_catalog_routing() -> Any:
    """PG-only catalog routing — no ES catalog envelope index for private catalogs."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
    )

    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _build_pg_only_collection_routing() -> Any:
    """PG-only collection routing — no ES collection envelope index for private catalogs."""
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
    )

    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _build_pg_only_asset_routing() -> Any:
    """PG-only asset routing — no public ES asset index for private catalogs.

    Without an explicit asset routing pin, assets inherit the platform
    default which augments WRITE/READ with the public
    ``asset_elasticsearch_driver``, leaking private assets into public
    search. Pinning PG-only WRITE/READ keeps assets out of the public
    index. The UPLOAD operation is intentionally omitted: it is
    auto-augmented at validation time from discoverable
    ``AssetUploadProtocol`` impls (see ``AssetRoutingConfig._self_register_drivers``),
    so we do not hardcode an upload driver here.
    """
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
    )

    return AssetRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


class PrivateCatalogPreset:
    """PG-only envelopes + per-tenant private ES on the items tier.

    Catalog and collection envelopes are stored in PostgreSQL only — no
    ES private index at those tiers. Assets are stored PG-only as well so
    they never reach the public asset Elasticsearch index. Items are
    indexed in the per-tenant private ES index
    (``{prefix}-{catalog_id}-private-items``). The items-private driver
    installs the catalog-wide DENY policy (``private_deny_{catalog_id}``)
    blocking public read access.
    """

    name = "private_catalog"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    description = (
        "PG-only catalog/collection envelopes + per-tenant private "
        "Elasticsearch indexer on the items tier. No anonymous read paths; "
        "IAM DENY (private_deny_{catalog_id}) blocks all_users on item "
        "URL patterns. No audience opt-ins. Use as the foundation for fully "
        "isolated tenant catalogs."
    )

    def build(self, catalog_id: str, **_scope: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle(
            catalog_routing=_build_pg_only_catalog_routing(),
            collection_template=_build_pg_only_collection_routing(),
            items_template=_build_private_items_routing(),
            asset_template=_build_pg_only_asset_routing(),
            audience_configs={},
        )
