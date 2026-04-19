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

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy
from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.modules.volumes.bounds import FeatureBounds
from dynastore.modules.volumes.tileset_builder import build_tileset
from dynastore.modules.volumes.writers.tileset_json import write_tileset_json

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
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tileset.json",
            self.get_tileset_json, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tiles/{tile_id}.b3dm",
            self.get_tile_b3dm, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/metadata",
            self.get_volumes_metadata, methods=["GET"],
        )

    async def get_tile_b3dm(
        self, catalog_id: str, collection_id: str, tile_id: str, request: Request,
    ):
        # Phase 5b will implement actual b3dm assembly. Until then, signal
        # that the surface is wired but the byte producer is not yet ready.
        raise HTTPException(
            status_code=501,
            detail=(
                "b3dm tile generation is not yet implemented (Phase 5b). "
                "tileset.json + endpoint surface available; tile content pending."
            ),
        )

    async def get_volumes_metadata(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        base = str(request.url).rstrip("/").rsplit("/", 1)[0]  # strip "/metadata"
        return {
            "title": f"3D GeoVolumes for {catalog_id}/{collection_id}",
            "description": "OGC API - 3D GeoVolumes (Cesium 3D Tiles encoding)",
            "config": {
                "max_features_per_tile": cfg.max_features_per_tile,
                "max_tree_depth": cfg.max_tree_depth,
                "root_geometric_error": cfg.root_geometric_error,
            },
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{base}/metadata"},
                {"rel": "data", "type": "application/json", "href": f"{base}/tileset.json"},
            ],
        }

    async def get_tileset_json(
        self, catalog_id: str, collection_id: str, request: Request,
    ) -> StreamingResponse:
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        # Phase 5c stub: empty feature list -> empty-skeleton tileset.json.
        # Phase 5d replaces this with bounds sourced from the geometries sidecar.
        bounds: list[FeatureBounds] = []
        base = str(request.url).rstrip("/").rsplit("/", 1)[0]  # strip "/tileset.json"
        template = f"{base}/tiles/{{tile_id}}.b3dm"
        tileset = build_tileset(bounds, cfg, content_uri_template=template)
        return StreamingResponse(
            write_tileset_json(tileset),
            media_type="application/json",
        )

    async def _get_volumes_config(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> VolumesConfig:
        """Resolve VolumesConfig via the platform's PluginConfig waterfall.

        Phase 5c falls back to defaults when no override exists. Phase 5d
        will hook the real ConfigsProtocol resolver once its registration
        pattern is confirmed.
        """
        # TODO(Phase 5d): replace with
        # `await get_protocol(ConfigsProtocol).get_config(VolumesConfig, ...)`
        return VolumesConfig()
