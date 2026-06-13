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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Cascade cleanup owner for stats telemetry docs.

:class:`StatsTelemetryOwner` deletes the Elasticsearch access-log index
``{prefix}_access_logs_{safe_catalog_id}`` when its catalog is hard-deleted.

Index naming follows :meth:`~.elasticsearch_storage.ElasticsearchStatsDriver._write_index`:
``{prefix}_access_logs_{catalog_id.lower().replace(" ", "_")}``

Only CATALOG scope is meaningful — the index is one-per-catalog and every doc
in it has ``catalog_id`` as its sole tenant discriminator.  COLLECTION- and
ASSET-scope deletes carry no stats partition to remove; ``describe_scope``
returns an empty list for those scopes.

Register via :func:`register_owners` from the stats module lifespan BEFORE
the CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)


def _safe_catalog_id(catalog_id: str) -> str:
    """Return the sanitised catalog_id used in the ES index name."""
    return catalog_id.lower().replace(" ", "_")


class StatsTelemetryOwner(BaseResourceOwner):
    """Purges the per-catalog access-log ES index on catalog hard-delete."""

    owner_id: ClassVar[str] = "stats.telemetry"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []

        from dynastore.modules.elasticsearch.client import get_index_prefix

        prefix = get_index_prefix()
        safe_id = _safe_catalog_id(scope_ref.catalog_id)
        index_name = f"{prefix}_access_logs_{safe_id}"

        return [
            CleanupRef(
                kind="stats_es_index",
                locator=index_name,
                owner_id=self.owner_id,
                metadata={
                    "catalog_id": scope_ref.catalog_id,
                    "index_name": index_name,
                },
            )
        ]

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE

        index_name = ref.metadata.get("index_name") or ref.locator
        catalog_id = ref.metadata.get("catalog_id", "")

        if dry_run:
            logger.info(
                "StatsTelemetryOwner: dry-run — would delete ES index %r (catalog=%r).",
                index_name, catalog_id,
            )
            return CleanupOutcome.DONE

        try:
            from dynastore.modules.elasticsearch.client import get_client

            es = get_client()
            if es is None:
                logger.warning(
                    "StatsTelemetryOwner: ES client unavailable — "
                    "cannot delete index %r (catalog=%r). Returning RETRY.",
                    index_name, catalog_id,
                )
                return CleanupOutcome.RETRY

            exists = await es.indices.exists(index=index_name)
            if not exists:
                logger.info(
                    "StatsTelemetryOwner: index %r already absent (catalog=%r) — DONE.",
                    index_name, catalog_id,
                )
                return CleanupOutcome.DONE

            await es.indices.delete(index=index_name)
            logger.info(
                "StatsTelemetryOwner: deleted ES index %r (catalog=%r).",
                index_name, catalog_id,
            )
            return CleanupOutcome.DONE

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "StatsTelemetryOwner: failed to delete ES index %r (catalog=%r): %s",
                index_name, catalog_id, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register stats cascade owners into *registry*.

    Call from the stats module lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(StatsTelemetryOwner())
    logger.info(
        "StatsModule: registered cascade owner %r.", StatsTelemetryOwner.owner_id
    )
