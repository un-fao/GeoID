"""OGC API - 3D GeoVolumes service (Phase 5c).

Phase 5c delivers HTTP endpoints + content negotiation. Tile content
(b3dm bytes from glTF assembly) lands in Phase 5b; until then the
``tiles/{tile_id}.b3dm`` endpoint returns 501.

Draft spec — URIs verified against the OGC 3D GeoVolumes 1.0 working
draft snapshot at the time of Pass 5 spec authorship. Re-verify before
production submission.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import APIRouter, FastAPI

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy

logger = logging.getLogger(__name__)


# Draft URIs — OGC API - 3D GeoVolumes Part 1 0.0 (working draft).
OGC_API_VOLUMES_URIS = [
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/3dtiles",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/tileset",
]


class VolumesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - 3D GeoVolumes extension."""

    priority: int = 170  # after Coverages (160), before any non-OGC

    conformance_uris = OGC_API_VOLUMES_URIS
    prefix = "/volumes"
    protocol_title = "DynaStore OGC API - 3D GeoVolumes"
    protocol_description = "Access to 3D tile data via OGC API - 3D GeoVolumes"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix=self.prefix, tags=["OGC API - 3D GeoVolumes"])
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.register_policies()
        logger.info("VolumesService: policies registered.")
        yield

    def register_policies(self):
        register_ogc_public_access_policy("volumes")

    def _register_routes(self) -> None:
        # Routes added in Tasks 2 + 3.
        pass
