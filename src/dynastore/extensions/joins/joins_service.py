"""OGC API - Joins service (Phase 4b PR-1).

Ships the OGC-conformant /join/* surface alongside the existing /dwh/*
(which is NOT touched). PR-1 supports `NamedSecondarySpec` only — the
secondary must reference a registered collection.

PR-2 will add `BigQuerySecondarySpec` (per-request target overrides +
Secret-wrapped credentials) on top of this surface.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import APIRouter, Body, FastAPI, Request

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy
from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import JoinRequest
from dynastore.modules.storage.router import resolve_drivers

logger = logging.getLogger(__name__)


async def _resolve_primary_driver(catalog_id: str, collection_id: str):
    """Resolve the first READ driver for the primary collection.

    Returns the driver instance (any CollectionItemsStore impl) or None
    if no driver is registered for this catalog/collection on READ.
    """
    drivers = await resolve_drivers(
        "READ", catalog_id, collection_id, hint="features",
    )
    return drivers[0].driver if drivers else None


async def _stream_primary_features(
    driver, *, catalog_id: str, collection_id: str,
    primary_column: str, limit: int = 100_000,
) -> AsyncIterator[Feature]:
    """Wrap any CollectionItemsStore driver's read_entities into the
    plain ``AsyncIterator[Feature]`` shape ``run_join`` expects.

    The ``primary_column`` is passed via ``context["id_column"]`` so
    drivers (e.g. BQ) that need to know which column carries the join
    key can use it for projection. Drivers that ignore context just
    return all columns — the executor reads ``primary_column`` from
    ``feature.properties`` either way.
    """
    async for feat in driver.read_entities(
        catalog_id, collection_id,
        limit=limit,
        context={"id_column": primary_column},
    ):
        yield feat


# Draft URIs — OGC API - Joins Part 1 0.0 (working draft).
OGC_API_JOINS_URIS = [
    "http://www.opengis.net/spec/ogcapi-joins-1/0.0/conf/core",
]


class JoinsService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - Joins extension."""

    priority: int = 180  # after Volumes (170)

    conformance_uris = OGC_API_JOINS_URIS
    prefix = "/join"
    protocol_title = "DynaStore OGC API - Joins"
    protocol_description = (
        "Per-request joins between a primary collection and a secondary "
        "(registered or per-request) data source."
    )

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix=self.prefix, tags=["OGC API - Joins"])
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.register_policies()
        logger.info("JoinsService: policies registered.")
        yield

    def register_policies(self):
        register_ogc_public_access_policy("join")

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.describe_join, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.execute_join, methods=["POST"],
        )

    async def describe_join(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        """Advertise supported secondary drivers + minimal capability surface."""
        base = str(request.url).rstrip("/")
        return {
            "title": "OGC API - Joins describe",
            "primary": {"catalog": catalog_id, "collection": collection_id},
            "supported_secondary_drivers": ["registered", "bigquery"],
            "links": [
                {"rel": "self", "type": "application/json", "href": base},
            ],
        }

    async def execute_join(
        self, catalog_id: str, collection_id: str, request: Request,
        body: JoinRequest = Body(...),
    ):
        """Execute the join.

        PR-1: stub for both driver types.
        PR-2: BigQuerySecondarySpec materializes the secondary side via
        Phase 4a's BQ driver; primary stream wiring lands next.
        """
        from dynastore.modules.joins import bq_secondary as bq_mod
        from dynastore.modules.joins.executor import index_secondary
        from dynastore.modules.joins.models import BigQuerySecondarySpec

        if isinstance(body.secondary, BigQuerySecondarySpec):
            secondary_index = await index_secondary(
                bq_mod.stream_bigquery_secondary(
                    body.secondary, secondary_column=body.join.secondary_column,
                ),
                secondary_column=body.join.secondary_column,
            )
            return {
                "type": "FeatureCollection",
                "features": [],
                "_phase4b_pr2_note": (
                    f"Secondary materialized: {len(secondary_index)} rows. "
                    "Primary stream wiring (resolve_drivers READ) lands next; "
                    "until then the join produces zero matches."
                ),
            }

        # NamedSecondarySpec — registered-ref resolution is a separate PR.
        return {
            "type": "FeatureCollection",
            "features": [],
            "_phase4b_pr1_note": (
                "Registered secondary resolution lands in a follow-up. "
                "Use BigQuerySecondarySpec for inline targets in this build."
            ),
        }
