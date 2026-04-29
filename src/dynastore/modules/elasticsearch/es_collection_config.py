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

"""Per-collection Elasticsearch configuration plugin.

Layered on top of :class:`ElasticsearchCatalogConfig`:

- ``ElasticsearchCatalogConfig.private`` is the catalog-tier default.
- ``ElasticsearchCollectionConfig.private`` is an optional per-collection
  override.  ``None`` (default) inherits the catalog value; explicit
  ``True`` / ``False`` pins the collection regardless of the catalog flag.

The resolver :func:`is_collection_private` reads the override-then-catalog
waterfall through ``ConfigsProtocol`` and returns the boolean an indexer
dispatch path should consult per write event.

This config is purely a READ surface for now — the existing classic indexer
remains the SSOT for catalog-level reindex behaviour.  Wiring the resolver
into the per-event dispatch (so a single collection can opt out of an
otherwise-private catalog at write time) is a follow-up step.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


async def _on_apply_es_collection_config(
    config: "ElasticsearchCollectionConfig",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Apply the per-collection ES override.

    Two effects happen on every collection-tier write:

    1. The cached resolver entry for ``(catalog_id, collection_id)`` is
       invalidated so the next call to :func:`is_collection_private`
       returns the new value without waiting for the 60s TTL.
    2. When the operator wrote an *explicit* override (``True`` or
       ``False`` — not ``None``), a single-collection reindex is
       dispatched in the matching mode so historical items in this
       collection are moved into the right ES index.  Reverting to
       ``private=None`` (inherit) is intentionally NOT auto-reindexed
       — the catalog-tier flag is unchanged so the existing index
       contents already match the catalog default; an operator who
       changes the catalog flag triggers the catalog-wide reindex
       through ``ElasticsearchCatalogConfig._on_apply``.

    Both effects are best-effort.  Cache invalidation failure falls
    back on the 60s TTL.  Reindex-dispatch failure leaves historical
    items in their pre-apply index — the operator can re-trigger via
    the dedicated ``elasticsearch_bulk_reindex_collection`` task.
    """
    if not catalog_id or not collection_id:
        return
    try:
        from dynastore.tools.cache import cache_invalidate

        cache_invalidate(
            is_collection_private, catalog_id, collection_id,
        )
    except Exception as exc:
        logger.debug(
            "ElasticsearchCollectionConfig: cache_invalidate failed for "
            "%s/%s (%s) — TTL expiry will still propagate within ~60s",
            catalog_id, collection_id, exc,
        )

    if config.private is None:
        # Reverted to inherit — catalog-tier unchanged means existing
        # indexed items already match the effective state.  No reindex.
        logger.info(
            "ElasticsearchCollectionConfig: %s/%s reverted to inherit "
            "catalog default; cache invalidated, no reindex dispatched.",
            catalog_id, collection_id,
        )
        return

    from dynastore.modules.elasticsearch.module import ElasticsearchModule

    es_module = get_protocol(ElasticsearchModule)
    if es_module is None:
        logger.debug(
            "ElasticsearchCollectionConfig: ElasticsearchModule not "
            "registered in this process — skipping reindex dispatch for "
            "%s/%s. Operator can re-trigger via the bulk reindex task.",
            catalog_id, collection_id,
        )
        return

    try:
        await es_module.bulk_reindex(
            catalog_id=catalog_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        logger.info(
            "ElasticsearchCollectionConfig: dispatched single-collection "
            "reindex for %s/%s after apply (regular driver target).",
            catalog_id, collection_id,
        )
    except Exception as exc:
        logger.warning(
            "ElasticsearchCollectionConfig: failed to dispatch reindex "
            "for %s/%s (%s) — items in the pre-apply index will remain "
            "stale until a manual elasticsearch_bulk_reindex_collection "
            "task is fired.",
            catalog_id, collection_id, exc,
        )


class ElasticsearchCollectionConfig(PluginConfig):
    """Per-collection Elasticsearch overrides.

    Editable at runtime via:
        PUT /configs/catalogs/{catalog_id}/collections/{collection_id}/elasticsearch
        body: {"private": true}    # or false, or omit to inherit

    Today this config carries one optional override (``private``);
    additional per-collection ES knobs can land here as needed (e.g.
    custom index name suffix, refresh-policy override).  It does NOT
    duplicate :class:`ElasticsearchCatalogConfig` — catalog-tier policy
    fields stay there.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("collection", "elasticsearch", None)
    _visibility: ClassVar[Optional[str]] = "collection"

    # Apply handler is registered imperatively at module-import time
    # (see ``ElasticsearchCollectionConfig.register_apply_handler(...)`` at
    # the bottom of this module).

    private: Optional[bool] = Field(
        default=None,
        description=(
            "Per-collection private indexing override.  ``None`` (default) "
            "inherits the catalog-tier ``ElasticsearchCatalogConfig."
            "private`` flag.  ``True`` forces this collection into "
            "the geoid-only private index regardless of the catalog "
            "default; ``False`` forces it into classic STAC indexing "
            "regardless of the catalog default.  Toggling this field "
            "invalidates the cached resolver entry for an instant effect "
            "on subsequent writes; existing indexed documents are NOT "
            "moved automatically — dispatch a single-collection reindex "
            "task if the override should backfill historical items."
        ),
    )


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


from dynastore.tools.cache import cached  # noqa: E402  -- after class def to avoid the apply handler closure capturing a not-yet-decorated callable


@cached(
    maxsize=1024,
    ttl=60,
    jitter=5,
    namespace="es_collection_private",
)
async def is_collection_private(
    catalog_id: str, collection_id: str,
) -> bool:
    """Return ``True`` iff ``collection_id`` in ``catalog_id`` should be
    indexed in Elasticsearch private mode.

    Resolution waterfall (highest precedence first):

    1. ``ElasticsearchCollectionConfig.private`` — when not ``None``,
       the collection-level explicit override wins.
    2. ``ElasticsearchCatalogConfig.private`` — the catalog default.
    3. ``False`` — when no config is registered at either tier.

    Cached with TTL=60s ± 5s jitter; the apply handler on the
    collection-tier config invalidates the entry on write.  The cache
    is keyed by the positional ``(catalog_id, collection_id)`` pair —
    no ``self`` to ignore (module-level function).
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.elasticsearch.es_catalog_config import (
        ElasticsearchCatalogConfig,
    )

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return False

    try:
        col_cfg = await configs.get_config(
            ElasticsearchCollectionConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception as exc:
        logger.debug(
            "is_collection_private: collection-tier fetch failed for "
            "%s/%s (%s) — falling through to catalog tier",
            catalog_id, collection_id, exc,
        )
        col_cfg = None

    if isinstance(col_cfg, ElasticsearchCollectionConfig) and col_cfg.private is not None:
        return col_cfg.private

    try:
        cat_cfg = await configs.get_config(
            ElasticsearchCatalogConfig, catalog_id=catalog_id,
        )
    except Exception as exc:
        logger.debug(
            "is_collection_private: catalog-tier fetch failed for "
            "%s (%s) — defaulting to False",
            catalog_id, exc,
        )
        return False

    if isinstance(cat_cfg, ElasticsearchCatalogConfig):
        return cat_cfg.private
    return False


# Apply-handler registration (Phase 1.5 — single registration path).
ElasticsearchCollectionConfig.register_apply_handler(_on_apply_es_collection_config)
