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
BigQueryMetadataTransformDriver — TRANSFORM-capability metadata driver.

Produces a partial collection-metadata envelope containing BigQuery-sourced
statistics (row counts, last-modified timestamps, etc.) under the
``bq_stats`` key.  The router's TRANSFORM chain merges this partial into the
main envelope when an endpoint opts into transform-aware output or the async
reindex pipeline is preparing a transformed INDEX / BACKUP envelope.

Replaces the deleted ``CollectionMetadataEnricherProtocol`` /
``BigQueryCollectionEnricher`` — one mechanism, one priority model, one SLA
model.  See role-based driver plan §Routing / §Transformer.

Activation: per-collection ``bq_stats`` config (via ``ConfigsProtocol``).
Absent config → returns ``None`` from ``get_metadata()``; the router skips
the partial entirely.
"""

import logging
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple, TYPE_CHECKING

from dynastore.models.protocols.driver_roles import DriverSla, MetadataDomain
from dynastore.models.protocols.metadata_driver import MetadataCapability
from dynastore.tools.discovery import get_protocol

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

logger = logging.getLogger(__name__)

BQ_STATS_CONFIG_ID = "bq_stats"


class BigQueryMetadataTransformDriver:
    """TRANSFORM driver backed by BigQuery, returning ``bq_stats`` partials.

    Structurally satisfies :class:`CollectionMetadataStore` (@runtime_checkable)
    but only declares the ``TRANSFORM`` capability — the router never picks
    this driver as a Primary for READ / WRITE / SEARCH.  Non-transform methods
    raise ``NotImplementedError`` to make any accidental invocation loud.

    Role-based driver attributes (plan §Driver roles):
    - ``domain``       — ``CORE`` (the ``bq_stats`` key is core-supplementary).
    - ``capabilities`` — ``{TRANSFORM}``.
    - ``sla``          — 2 s timeout, ``degrade`` on timeout (BigQuery is
                          high-variance; never block the hot path for it).
    """

    # --- Role-based driver attributes ---------------------------------------
    capabilities: FrozenSet[str] = frozenset({MetadataCapability.TRANSFORM})
    domain: ClassVar[MetadataDomain] = MetadataDomain.CORE
    sla: ClassVar[DriverSla] = DriverSla(
        timeout_ms=2000,
        on_timeout="degrade",
        required=False,
    )

    # --- TRANSFORM entry point ----------------------------------------------

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a partial envelope with BQ stats, or ``None`` if not configured.

        When the router's TRANSFORM chain invokes this driver, the returned
        dict is merged into the main envelope.  Empty / None → no contribution.
        """
        from dynastore.models.protocols import BigQueryProtocol, ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return None

        try:
            stats_config = await configs.get_config(
                BQ_STATS_CONFIG_ID,  # type: ignore[arg-type]
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return None  # no config → nothing to contribute

        if not stats_config:
            return None

        bq = get_protocol(BigQueryProtocol)
        if not bq:
            logger.debug(
                "BigQueryMetadataTransformDriver: BigQueryProtocol not available for %s/%s",
                catalog_id, collection_id,
            )
            return None

        query = getattr(stats_config, "query", None)
        project_id = getattr(stats_config, "project_id", None)
        if not query or not project_id:
            return None

        try:
            records = await bq.execute_query(query, project_id)
        except Exception as exc:
            logger.warning(
                "BigQueryMetadataTransformDriver query failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return None

        if not records:
            return None
        return {"bq_stats": records[0]}

    # --- CollectionMetadataStore protocol stubs -----------------------------
    # TRANSFORM-only driver — these must exist for structural-typing checks
    # (@runtime_checkable), but must never be invoked.  A loud error makes
    # misrouting detectable in tests and staging.

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            "BigQueryMetadataTransformDriver is TRANSFORM-only; "
            "route writes to a Primary driver."
        )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            "BigQueryMetadataTransformDriver is TRANSFORM-only; "
            "route deletes to a Primary driver."
        )

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        raise NotImplementedError(
            "BigQueryMetadataTransformDriver is TRANSFORM-only; "
            "route searches to an Indexer or Primary with SEARCH capability."
        )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """TRANSFORM driver has no driver-specific config waterfall entry."""
        return None

    async def is_available(self) -> bool:
        """Health check — available iff the BigQueryProtocol plugin is registered."""
        from dynastore.models.protocols import BigQueryProtocol

        return get_protocol(BigQueryProtocol) is not None

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        raise NotImplementedError(
            "BigQueryMetadataTransformDriver does not declare PHYSICAL_ADDRESSING."
        )


# ---------------------------------------------------------------------------
# Back-compat shim — the old symbol name is still imported by gcp_module and
# by at least one notebook.  Keep it as a direct alias of the new class until
# M3 lands the async reindex pipeline and downstream renames settle.  Remove
# this alias in a follow-up PR once all call sites are updated.
# ---------------------------------------------------------------------------

BigQueryCollectionEnricher = BigQueryMetadataTransformDriver
