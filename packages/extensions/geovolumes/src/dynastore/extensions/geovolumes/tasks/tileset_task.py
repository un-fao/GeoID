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

"""GeoVolumesTilesetTask — OGC Process that generates 3D Tiles 1.1 from stored CityJSON items.

Workflow:
  1. Load collection extras to recover the CityJSON transform/EPSG.
  2. Stream all items from PostgreSQL; extract the ``cityjson`` extras field.
  3. Partition into batches (default MAX_FEATURES_PER_TILE); one GLB per batch.
  4. Serialise tileset.json.
  5. Upload GLBs + tileset.json via TileArchiveStorageProtocol (GCS / local).
  6. Register tileset.json as a collection ASSET with roles ["3dtiles", "tileset"].

No runtime DDL is performed.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Optional

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.tiles.tiles_module import TileArchiveStorageProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

from .definition import GEOVOLUMES_TILESET_PROCESS_DEFINITION
from .models import GeoVolumesTilesetRequest
from .tileset_builder import build_glb, build_tileset_json, export_tileset_bytes
from dynastore.extensions.geovolumes.cityjson_ingest import CityJsonHeader

logger = logging.getLogger(__name__)

MAX_FEATURES_PER_TILE = 50_000


def _get_item_service(catalog_module: Any) -> Any:
    """Return the ItemsProtocol from the catalog module."""
    return catalog_module.items


def _build_header_from_extras(extras: dict[str, Any]) -> CityJsonHeader:
    """Reconstruct a CityJsonHeader from the collection extras dict."""
    import re

    transform = extras.get("cityjson:transform", {})
    ref_sys = extras.get("cityjson:referenceSystem")

    epsg: Optional[int] = None
    if ref_sys:
        m = re.search(r"(\d+)\s*$", ref_sys)
        epsg = int(m.group(1)) if m else None

    return CityJsonHeader(
        version=extras.get("cityjson:version", "2.0"),
        transform_scale=transform.get("scale", [1.0, 1.0, 1.0]),
        transform_translate=transform.get("translate", [0.0, 0.0, 0.0]),
        reference_system=ref_sys,
        epsg=epsg,
    )


def _collect_bbox_from_features(
    features: list[dict[str, Any]], header: CityJsonHeader
) -> tuple[float, float, float, float]:
    """Compute a rough WGS84 bbox from a list of CityJSONFeature dicts."""
    import pyproj
    from dynastore.extensions.geovolumes.cityjson_ingest import dequantize

    if header.epsg is None:
        return (-180.0, -90.0, 180.0, 90.0)

    transformer = pyproj.Transformer.from_crs(header.epsg, 4326, always_xy=True)
    lons: list[float] = []
    lats: list[float] = []
    for feat in features:
        for v in dequantize(feat.get("vertices", []), header):
            lon, lat = transformer.transform(v[0], v[1])
            lons.append(lon)
            lats.append(lat)
    if not lons:
        return (-180.0, -90.0, 180.0, 90.0)
    return (min(lons), min(lats), max(lons), max(lats))


class GeoVolumesTilesetTask(
    ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]
):
    """OGC Process: generate a 3D Tiles 1.1 tileset from CityJSON items."""

    @staticmethod
    def get_definition() -> Process:
        return GEOVOLUMES_TILESET_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None) -> None:
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[ExecuteRequest]
    ) -> Optional[StatusInfo]:
        request = GeoVolumesTilesetRequest.model_validate(payload.inputs.inputs)

        catalog_module = get_protocol(CatalogsProtocol)
        if catalog_module is None:
            raise RuntimeError("CatalogsProtocol is unavailable.")

        archive_storage = get_protocol(TileArchiveStorageProtocol)
        if archive_storage is None:
            raise RuntimeError("TileArchiveStorageProtocol is unavailable.")

        catalog_id = request.catalog_id
        collection_id = request.collection_id
        lod_filter = request.lod

        collection = await catalog_module.get_collection(catalog_id, collection_id)
        extras: dict[str, Any] = getattr(collection, "extras", {}) or {}
        header = _build_header_from_extras(extras)

        item_service = _get_item_service(catalog_module)

        features = await self._load_cityjson_features(
            item_service, catalog_id, collection_id
        )
        logger.info(
            "Loaded %d CityJSON features for %s/%s",
            len(features),
            catalog_id,
            collection_id,
        )

        bbox = _collect_bbox_from_features(features, header)

        glb_refs, tile_count = await self._build_and_upload_glbs(
            archive_storage=archive_storage,
            features=features,
            header=header,
            catalog_id=catalog_id,
            collection_id=collection_id,
            lod_filter=lod_filter,
        )

        tileset = build_tileset_json(bbox, glb_refs)
        tileset_bytes = export_tileset_bytes(tileset)
        tileset_uri = await self._upload_tileset_json(
            archive_storage=archive_storage,
            tileset_bytes=tileset_bytes,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

        await self._register_tileset_asset(
            catalog_module=catalog_module,
            catalog_id=catalog_id,
            collection_id=collection_id,
            tileset_uri=tileset_uri,
        )

        logger.info(
            "3D Tiles generation complete: %s (%d tiles, %d features)",
            tileset_uri,
            tile_count,
            len(features),
        )
        return None

    async def _load_cityjson_features(
        self,
        item_service: Any,
        catalog_id: str,
        collection_id: str,
    ) -> list[dict[str, Any]]:
        """Stream items from PG and collect the stored CityJSONFeature dicts."""
        from dynastore.models.query_builder import QueryRequest

        query = QueryRequest()
        # stream_items returns a QueryResponse (awaitable); its __aiter__ yields Feature objects.
        # In some contexts it may return an async generator directly.
        response = item_service.stream_items(catalog_id, collection_id, query)
        if hasattr(response, "__await__"):
            response = await response

        features: list[dict[str, Any]] = []
        async for item in response:
            cityjson = None
            extras = getattr(item, "extras", None)
            if extras and "cityjson" in extras:
                cityjson = extras["cityjson"]
            if cityjson is None:
                props = getattr(item, "properties", None)
                if props and "cityjson" in props:
                    cityjson = props["cityjson"]
            if cityjson is not None:
                features.append(cityjson)
        return features

    async def _build_and_upload_glbs(
        self,
        *,
        archive_storage: Any,
        features: list[dict[str, Any]],
        header: CityJsonHeader,
        catalog_id: str,
        collection_id: str,
        lod_filter: str | None,
    ) -> tuple[list[str], int]:
        """Build one or more GLBs and upload them; return (glb_refs, tile_count)."""
        glb_refs: list[str] = []
        batch_size = MAX_FEATURES_PER_TILE

        batches = [
            features[i : i + batch_size]
            for i in range(0, max(len(features), 1), batch_size)
        ]
        if not batches:
            batches = [[]]

        for idx, batch in enumerate(batches):
            glb_bytes = build_glb(batch, header, lod_filter)
            filename = f"tile_{idx}.glb"
            glb_io = io.BytesIO(glb_bytes)
            glb_io.name = filename
            uri = await archive_storage.save_archive(
                catalog_id, collection_id, filename, glb_io
            )
            glb_refs.append(filename)
            logger.debug("Uploaded GLB: %s", uri)

        return glb_refs, len(batches)

    async def _upload_tileset_json(
        self,
        *,
        archive_storage: Any,
        tileset_bytes: bytes,
        catalog_id: str,
        collection_id: str,
    ) -> str:
        """Upload tileset.json and return its storage URI."""
        ts_io = io.BytesIO(tileset_bytes)
        ts_io.name = "tileset.json"
        uri: str = await archive_storage.save_archive(
            catalog_id, collection_id, "tileset.json", ts_io
        )
        return uri

    async def _register_tileset_asset(
        self,
        *,
        catalog_module: Any,
        catalog_id: str,
        collection_id: str,
        tileset_uri: str,
    ) -> None:
        """Register tileset.json as a collection asset with roles 3dtiles/tileset."""
        from dynastore.modules.catalog.asset_service import VirtualAssetCreate

        asset_manager = catalog_module.assets
        asset_id = f"3dtiles-{collection_id}"
        payload = VirtualAssetCreate(
            asset_id=asset_id,
            href=tileset_uri,
            metadata={"roles": ["3dtiles", "tileset"]},
        )
        await asset_manager.create_asset(catalog_id, payload, collection_id)
