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

"""OGC Dimensions extension — exposes dimension generators as REST API
and materializes dimension members as OGC API - Records.

Wraps the ogc-dimensions package (pip dependency) into a Dynastore
ExtensionProtocol so it can be deployed on the tools Cloud Run service.

At startup the extension:

1. Registers all dimension generators in the ogc-dimensions router
   (live API at ``/dimensions/{id}/members``, ``/inverse``, etc.).
2. Materializes every dimension's members into a RECORDS-type collection
   so they are also browsable via the OGC API - Records extension at
   ``/records/catalogs/_dimensions_/collections/{dim_id}/items``.

The extension declares OGC API - Dimensions conformance as a
**profile of OGC API - Records**, packaged as OGC Building Blocks:

- ``dimension-collection`` — a Records catalogue with a generator object
- ``dimension-member`` — a Record with ``time.interval`` + ``labels`` map
- ``dimension-pagination`` — OGC Common Part 2 pagination profile
- ``dimension-inverse`` — value → member mapping (new conformance class)
- ``dimension-hierarchical`` — children / ancestors navigation
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.conformance import register_conformance_uris

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIMENSIONS_CATALOG_ID = "_dimensions_"
"""Internal catalog ID for materialized dimension collections."""

UPSERT_BATCH_SIZE = 500
"""Number of records to upsert per batch to avoid memory spikes."""

# ---------------------------------------------------------------------------
# OGC Dimensions conformance URIs (profile of OGC API - Records)
# ---------------------------------------------------------------------------
OGC_DIMENSIONS_URIS = [
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/core",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-collection",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-member",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-pagination",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-inverse",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-hierarchical",
]


# ---------------------------------------------------------------------------
# Member → Feature conversion
# ---------------------------------------------------------------------------


def _member_to_feature(
    member: Any,
    dim_name: str,
    dim_type: str,
) -> Dict[str, Any]:
    """Convert a ``GeneratedMember`` to a GeoJSON Feature dict (Record).

    Parameters
    ----------
    member
        A ``GeneratedMember`` from the ogc-dimensions generator.
    dim_name
        Dimension identifier (e.g. ``"temporal-dekadal"``).
    dim_type
        Dimension type string (``"temporal"``, ``"nominal"``, ``"ordinal"``).
    """
    code = member.code or str(member.value)
    extra = member.extra or {}

    props: Dict[str, Any] = {
        "title": extra.get("label") or code,
        "recordType": "dimension-member",
        "dimension:type": dim_type,
        "dimension:code": code,
        "dimension:index": member.index,
    }

    # Temporal interval
    if member.start and member.end:
        props["valid_from"] = str(member.start)
        props["valid_to"] = str(member.end)

    # Multilingual labels
    labels = extra.get("labels")
    if labels and isinstance(labels, dict):
        props["labels"] = labels

    # Hierarchy
    parent_code = extra.get("parent_code")
    if parent_code is not None:
        props["dimension:parent"] = parent_code
    level = extra.get("level")
    if level is not None:
        props["dimension:level"] = level
    if member.has_children:
        props["dimension:has_children"] = True

    # Extra fields (unit, etc.) — pass through
    for key in ("unit", "rank"):
        if key in extra:
            props[key] = extra[key]

    # Lower/upper for integer-range
    if "lower" in extra:
        props["dimension:start"] = extra["lower"]
    if "upper" in extra:
        props["dimension:end"] = extra["upper"]

    return {
        "type": "Feature",
        "id": code,
        "geometry": None,
        "properties": props,
    }


def _infer_dim_type(generator: Any) -> str:
    """Infer the dimension type string from the generator class."""
    cls_name = type(generator).__name__.lower()
    if "period" in cls_name or "temporal" in cls_name:
        return "temporal"
    if "integer" in cls_name or "range" in cls_name:
        return "ordinal"
    if "leveled" in cls_name:
        return "nominal"
    if "tree" in cls_name:
        return "nominal"
    return "other"


# ---------------------------------------------------------------------------
# Materialization helpers
# ---------------------------------------------------------------------------


async def _enumerate_all_members(
    generator: Any,
    extent_min: str,
    extent_max: str,
    dim_name: str,
    dim_type: str,
) -> List[Dict[str, Any]]:
    """Enumerate all members from a generator, handling pagination.

    For temporal/integer generators with large extents this paginates
    through the full sequence.  For tree generators the total is bounded
    by the node count.
    """
    features: List[Dict[str, Any]] = []
    page_size = 500
    offset = 0

    while True:
        result = generator.generate(
            extent_min, extent_max, limit=page_size, offset=offset,
        )
        for m in result.members:
            features.append(_member_to_feature(m, dim_name, dim_type))

        if result.number_returned < page_size:
            break
        offset += page_size

    return features


async def _materialize_dimension(
    catalogs: Any,
    dim_name: str,
    dim_config: Any,
    db_resource: Any,
) -> int:
    """Create a RECORDS collection and upsert all dimension members.

    Returns the number of records materialized.
    """
    generator = dim_config.generator
    dim_type = _infer_dim_type(generator)

    # Create RECORDS collection (idempotent — skips if exists)
    existing = await catalogs.get_collection(
        DIMENSIONS_CATALOG_ID, dim_name, lang="en",
    )
    if not existing:
        await catalogs.create_collection(
            DIMENSIONS_CATALOG_ID,
            {
                "id": dim_name,
                "title": dim_name.replace("-", " ").title(),
                "description": dim_config.description,
                "layer_config": {"collection_type": "RECORDS"},
            },
            db_resource=db_resource,
        )
        logger.info("Created RECORDS collection: %s/%s", DIMENSIONS_CATALOG_ID, dim_name)

    # Generate all members
    features = await _enumerate_all_members(
        generator, dim_config.extent_min, dim_config.extent_max, dim_name, dim_type,
    )

    if not features:
        return 0

    # Batch upsert
    total = 0
    for i in range(0, len(features), UPSERT_BATCH_SIZE):
        batch = features[i : i + UPSERT_BATCH_SIZE]
        await catalogs.upsert(
            DIMENSIONS_CATALOG_ID, dim_name, batch, db_resource=db_resource,
        )
        total += len(batch)
        logger.debug(
            "Upserted batch %d–%d for %s (%d total)",
            i, i + len(batch), dim_name, total,
        )

    return total


async def _materialize_all_dimensions(dimensions: Dict[str, Any]) -> None:
    """Materialize all registered dimensions as Records collections.

    Uses ``managed_transaction`` to get a DB connection outside of
    request context.
    """
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.tools.discovery import get_protocol
    from dynastore.tools.protocol_helpers import get_engine

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("CatalogsProtocol not available — skipping dimension materialization.")
        return

    engine = get_engine()
    if not engine:
        logger.warning("DB engine not available — skipping dimension materialization.")
        return

    try:
        async with managed_transaction(engine) as conn:
            # Ensure dimensions catalog exists
            await catalogs.ensure_catalog_exists(
                DIMENSIONS_CATALOG_ID, db_resource=conn,
            )
    except Exception as exc:
        logger.error("Failed to ensure dimensions catalog: %s", exc)
        return

    grand_total = 0
    for dim_name, dim_config in dimensions.items():
        try:
            async with managed_transaction(engine) as conn:
                count = await _materialize_dimension(
                    catalogs, dim_name, dim_config, db_resource=conn,
                )
                grand_total += count
                logger.info(
                    "Materialized %d records for dimension '%s'", count, dim_name,
                )
        except Exception as exc:
            logger.error(
                "Failed to materialize dimension '%s': %s", dim_name, exc,
                exc_info=True,
            )

    logger.info(
        "Dimension materialization complete: %d total records across %d dimensions.",
        grand_total,
        len(dimensions),
    )


# ---------------------------------------------------------------------------
# Extension
# ---------------------------------------------------------------------------


class DimensionsExtension(ExtensionProtocol):
    priority: int = 200

    def __init__(self, app: FastAPI):
        self.app = app

        from ogc_dimensions.api.routes import DIMENSIONS, DimensionConfig, router as dimensions_router
        from ogc_dimensions.generators import (
            DailyPeriodGenerator,
            IntegerRangeGenerator,
            StaticTreeGenerator,
            LeveledTreeGenerator,
        )

        from .use_cases import ADMIN_NODES, INDICATOR_NODES, SPECIES_NODES

        # -- Temporal pagination demos (100-year extents) ----------------------
        #
        # Three non-Gregorian calendars in wide operational use for agri/climate
        # monitoring demonstrate both pagination (large member counts) and a
        # real interoperability problem: the same "5-day period" concept is
        # encoded with two incompatible calendar systems depending on the data
        # producer.  Clients cannot combine pentadal datasets without knowing
        # which system was used.
        #
        # Reference: https://github.com/ccancellieri/ogc-dimensions/tree/main/spec

        DIMENSIONS["temporal-dekadal"] = DimensionConfig(
            generator=DailyPeriodGenerator(period_days=10, scheme="monthly"),
            description=(
                "Dekadal temporal dimension — 10-day periods, 36 per year, "
                "month-aligned (D1=1-10, D2=11-20, D3=remainder). "
                "Widely used for agricultural monitoring and early warning systems. "
                "100-year extent (1950-2050) demonstrates OGC-style pagination over "
                "large temporal dimensions (3 600+ members)."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )
        DIMENSIONS["temporal-pentadal-monthly"] = DimensionConfig(
            generator=DailyPeriodGenerator(period_days=5, scheme="monthly"),
            description=(
                "Pentadal-monthly temporal dimension — 5-day periods, 72 per year, "
                "month-aligned (P1=1-5, P2=6-10, ..., P6=26-EOM). "
                "Used by rainfall estimation products that align dekads and pentads "
                "to the same month boundaries (e.g. CHIRPS, CDT). "
                "100-year extent yields 7 200+ members; illustrates that pentadal "
                "and dekadal periods from the same producer are directly comparable."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )
        DIMENSIONS["temporal-pentadal-annual"] = DimensionConfig(
            generator=DailyPeriodGenerator(period_days=5, scheme="annual"),
            description=(
                "Pentadal-annual temporal dimension — 5-day periods, 73 per year, "
                "year-start-aligned (P1=Jan 1-5, ..., P73=Dec 27-31). "
                "Used by global precipitation climatology products that count pentads "
                "from January 1 regardless of month boundaries (e.g. GPCP, CPC/NOAA). "
                "Interoperability note: pentad #12 in the monthly system and pentad "
                "#12 in the annual system refer to different calendar intervals — "
                "a client must know which encoding was used before combining datasets."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )

        # -- Hierarchical dimensions -------------------------------------------
        DIMENSIONS["indicator-tree"] = DimensionConfig(
            generator=StaticTreeGenerator(nodes=INDICATOR_NODES),
            description=(
                "Statistical indicator tree. "
                "Recursive hierarchy: Domain -> Group -> Indicator."
            ),
            extent_min="",
            extent_max="",
        )
        DIMENSIONS["admin-boundaries"] = DimensionConfig(
            generator=LeveledTreeGenerator(nodes=ADMIN_NODES),
            description=(
                "Administrative boundaries. Leveled hierarchy: "
                "Continent (L0) -> Country (L1) -> Region (L2). "
                "Supports ?level= filter."
            ),
            extent_min="",
            extent_max="",
        )
        DIMENSIONS["forestry-species"] = DimensionConfig(
            generator=StaticTreeGenerator(nodes=SPECIES_NODES),
            description=(
                "Forestry species classification. "
                "Recursive hierarchy with search (exact + like)."
            ),
            extent_min="",
            extent_max="",
        )

        # -- Integer range demo ------------------------------------------------
        DIMENSIONS["elevation-bands"] = DimensionConfig(
            generator=IntegerRangeGenerator(step=50),
            description=(
                "Elevation bands (50 m step, 0-8848 m). "
                "Invertible, searchable, supports /inverse."
            ),
            extent_min="0",
            extent_max="8848",
        )

        self._dimensions = DIMENSIONS

        self.router = APIRouter(prefix="/dimensions", tags=["OGC Dimensions"])
        self.router.include_router(dimensions_router)

        register_conformance_uris(OGC_DIMENSIONS_URIS)

        logger.info(
            "OGC Dimensions extension loaded — %d dimensions registered",
            len(DIMENSIONS),
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        register_conformance_uris(OGC_DIMENSIONS_URIS)

        # Materialize all dimension members as Records
        await _materialize_all_dimensions(self._dimensions)

        logger.info(
            "DimensionsExtension: conformance + materialization complete "
            "(OGC Dimensions profile of OGC API - Records)."
        )
        yield
