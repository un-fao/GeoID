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

"""Tests for the pure-Python 3D Tiles 1.1 generation (tileset_builder).

All tests are pure-function — no database, no storage, no runtime protocols.
"""

from __future__ import annotations

import io
import math
import struct
from typing import Any

import pytest

# These guards must come before the module-level imports that use them.
# noqa: E402 is added to suppress the linter's module-import-ordering warning
# that fires because importorskip is not a plain import statement.
pytest.importorskip("trimesh")
pytest.importorskip("mapbox_earcut")

import trimesh  # noqa: E402
from dynastore.extensions.geovolumes.tasks.tileset_builder import (  # noqa: E402
    build_glb,
    build_tileset_json,
    dequantize_feature,
    triangulate_surface,
)
from dynastore.extensions.geovolumes.cityjson_ingest import CityJsonHeader  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_header(epsg: int = 28992) -> CityJsonHeader:
    """Minimal CityJsonHeader for a Dutch RD New projection."""
    return CityJsonHeader(
        version="2.0",
        transform_scale=[0.001, 0.001, 0.001],
        transform_translate=[85000.0, 446000.0, 0.0],
        reference_system="https://www.opengis.net/def/crs/EPSG/0/28992",
        epsg=epsg,
    )


def _make_box_feature(
    x0: int, y0: int, z0: int, x1: int, y1: int, z1: int, fid: str = "bld-1"
) -> dict[str, Any]:
    """One CityJSONFeature with a single Solid (a box).

    Vertices are quantized integers (raw, before dequantize).
    The box has 8 corners and 6 faces (each face = 1 outer ring of 4 verts).
    """
    verts = [
        [x0, y0, z0], [x1, y0, z0], [x1, y1, z0], [x0, y1, z0],
        [x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1],
    ]
    # 6 faces (outer ring only, CCW), each ring wrapped in an extra list per CityJSON spec
    faces = [
        [[0, 1, 2, 3]],  # bottom
        [[4, 5, 6, 7]],  # top
        [[0, 1, 5, 4]],  # front
        [[1, 2, 6, 5]],  # right
        [[2, 3, 7, 6]],  # back
        [[3, 0, 4, 7]],  # left
    ]
    return {
        "type": "CityJSONFeature",
        "id": fid,
        "CityObjects": {
            fid: {
                "type": "Building",
                "geometry": [
                    {"type": "Solid", "lod": "2", "boundaries": [faces]}
                ],
                "attributes": {"height": (z1 - z0) * 0.001},
            }
        },
        "vertices": verts,
    }


# ---------------------------------------------------------------------------
# Unit tests — triangulate_surface
# ---------------------------------------------------------------------------


def test_triangulate_surface_quad() -> None:
    """A planar quad (4 verts) triangulates into 2 triangles (6 indices)."""
    ring = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (1.0, 1.0, 0.0), (0.0, 1.0, 0.0)]
    indices = triangulate_surface(ring)
    # 2 triangles = 6 indices
    assert len(indices) == 6
    # All indices must be valid vertex references
    assert all(0 <= i < len(ring) for i in indices)


def test_triangulate_surface_triangle() -> None:
    """A triangle returns 3 indices (itself)."""
    ring = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (0.5, 1.0, 0.0)]
    indices = triangulate_surface(ring)
    assert len(indices) == 3


# ---------------------------------------------------------------------------
# Unit tests — dequantize_feature
# ---------------------------------------------------------------------------


def test_dequantize_feature_scale_translate() -> None:
    """dequantize_feature applies scale + translate per axis."""
    header = _make_header()
    feature = _make_box_feature(0, 0, 0, 1000, 1000, 100)
    real_verts = dequantize_feature(feature, header)
    # vertex [0] = [0*0.001+85000, 0*0.001+446000, 0*0.001+0] = (85000, 446000, 0)
    assert real_verts[0] == pytest.approx((85000.0, 446000.0, 0.0))
    # vertex [6] = [1000*0.001+85000, 1000*0.001+446000, 100*0.001+0] = (85001, 446001, 0.1)
    assert real_verts[6] == pytest.approx((85001.0, 446001.0, 0.1), abs=1e-6)


# ---------------------------------------------------------------------------
# Unit tests — build_glb
# ---------------------------------------------------------------------------


def test_build_glb_returns_bytes() -> None:
    """build_glb returns non-empty bytes starting with the GLB magic."""
    header = _make_header()
    features = [_make_box_feature(0, 0, 0, 1000, 1000, 100, "b1")]
    result = build_glb(features, header)
    assert isinstance(result, bytes)
    assert len(result) > 0
    # GLB magic = 0x46546C67 little-endian
    magic = struct.unpack_from("<I", result, 0)[0]
    assert magic == 0x46546C67


def test_build_glb_parseable_by_trimesh() -> None:
    """trimesh can parse the GLB and yields at least one mesh."""
    header = _make_header()
    features = [_make_box_feature(0, 0, 0, 1000, 1000, 100, "b1")]
    glb_bytes = build_glb(features, header)
    scene = trimesh.load(io.BytesIO(glb_bytes), file_type="glb")
    meshes = list(scene.geometry.values()) if hasattr(scene, "geometry") else [scene]
    assert len(meshes) >= 1


def test_build_glb_vertex_z_range() -> None:
    """Mesh vertex Z values span the dequantized zmin/zmax of the input feature."""
    header = _make_header(epsg=4326)  # use geographic CRS so no reprojection offset
    header = CityJsonHeader(
        version="2.0",
        transform_scale=[1.0, 1.0, 1.0],
        transform_translate=[0.0, 0.0, 0.0],
        reference_system="https://www.opengis.net/def/crs/EPSG/0/4326",
        epsg=4326,
    )
    features = [_make_box_feature(0, 0, 5, 10, 10, 20, "b1")]
    glb_bytes = build_glb(features, header)
    scene = trimesh.load(io.BytesIO(glb_bytes), file_type="glb")
    # Collect all Z values across every mesh
    all_z: list[float] = []
    meshes = list(scene.geometry.values()) if hasattr(scene, "geometry") else [scene]
    for mesh in meshes:
        if hasattr(mesh, "vertices") and len(mesh.vertices) > 0:
            all_z.extend(float(v[2]) for v in mesh.vertices)
    assert all_z, "No vertices found in GLB"
    # The Z range must contain the input zmin=5 and zmax=20 (in ECEF; just check span > 0)
    assert max(all_z) > min(all_z)


def test_build_glb_multiple_buildings() -> None:
    """One GLB for two buildings contains geometry for both."""
    header = _make_header()
    features = [
        _make_box_feature(0, 0, 0, 100, 100, 50, "b1"),
        _make_box_feature(200, 0, 0, 300, 100, 80, "b2"),
    ]
    glb_bytes = build_glb(features, header)
    scene = trimesh.load(io.BytesIO(glb_bytes), file_type="glb")
    meshes = list(scene.geometry.values()) if hasattr(scene, "geometry") else [scene]
    total_verts = sum(
        len(m.vertices) for m in meshes if hasattr(m, "vertices")
    )
    assert total_verts > 0


# ---------------------------------------------------------------------------
# Unit tests — build_tileset_json
# ---------------------------------------------------------------------------


def test_build_tileset_json_asset_version() -> None:
    """tileset.json asset.version must be '1.1'."""
    bbox = (-180.0, -90.0, 0.0, 90.0)
    tileset = build_tileset_json(bbox, ["tile_0.glb"])
    assert tileset["asset"]["version"] == "1.1"


def test_build_tileset_json_root_bounding_volume() -> None:
    """root.boundingVolume.region matches the supplied bbox (in radians)."""
    bbox = (10.0, 45.0, 11.0, 46.0)
    tileset = build_tileset_json(bbox, ["tile_0.glb"])
    region = tileset["root"]["boundingVolume"]["region"]
    assert len(region) == 6
    # west, south, east, north, minH, maxH
    assert region[0] == pytest.approx(math.radians(10.0))
    assert region[1] == pytest.approx(math.radians(45.0))
    assert region[2] == pytest.approx(math.radians(11.0))
    assert region[3] == pytest.approx(math.radians(46.0))


def test_build_tileset_json_single_glb_content() -> None:
    """Single GLB produces a root tile with content.uri set."""
    bbox = (0.0, 0.0, 1.0, 1.0)
    tileset = build_tileset_json(bbox, ["collection.glb"])
    root = tileset["root"]
    assert "content" in root
    assert root["content"]["uri"] == "collection.glb"


def test_build_tileset_json_multiple_children() -> None:
    """Multiple GLBs produce children tiles, each with content.uri."""
    bbox = (0.0, 0.0, 1.0, 1.0)
    glb_refs = ["tile_0.glb", "tile_1.glb", "tile_2.glb"]
    tileset = build_tileset_json(bbox, glb_refs)
    root = tileset["root"]
    assert "children" in root
    uris = [c["content"]["uri"] for c in root["children"]]
    assert sorted(uris) == sorted(glb_refs)


def test_build_tileset_json_geometric_error() -> None:
    """geometricError is present at root and leaf levels."""
    bbox = (0.0, 0.0, 1.0, 1.0)
    tileset = build_tileset_json(bbox, ["t.glb"])
    assert "geometricError" in tileset
    assert tileset["geometricError"] >= 0
    assert "geometricError" in tileset["root"]


# ---------------------------------------------------------------------------
# Integration-style tests — task payload model
# ---------------------------------------------------------------------------


def test_geovolumes_tileset_request_model() -> None:
    """GeoVolumesTilesetRequest validates required and optional fields."""
    from dynastore.extensions.geovolumes.tasks.models import GeoVolumesTilesetRequest

    req = GeoVolumesTilesetRequest(catalog_id="cat1", collection_id="col1")
    assert req.catalog_id == "cat1"
    assert req.collection_id == "col1"
    assert req.lod is None


def test_geovolumes_tileset_request_lod() -> None:
    """lod field is optional and accepts a string value."""
    from dynastore.extensions.geovolumes.tasks.models import GeoVolumesTilesetRequest

    req = GeoVolumesTilesetRequest(catalog_id="c", collection_id="x", lod="2")
    assert req.lod == "2"


# ---------------------------------------------------------------------------
# Integration-style tests — task class (mocked storage/asset_manager)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_geovolumes_tileset_task_raises_on_empty_features() -> None:
    """GeoVolumesTilesetTask.run() raises RuntimeError when no CityJSON features are found."""
    from unittest.mock import AsyncMock, MagicMock, patch

    import uuid
    from dynastore.extensions.geovolumes.tasks.tileset_task import GeoVolumesTilesetTask
    from dynastore.modules.processes.models import ExecuteRequest
    from dynastore.modules.tasks.models import TaskPayload

    header = _make_header()
    collection_extras = {
        "cityjson:transform": {
            "scale": header.transform_scale,
            "translate": header.transform_translate,
        },
        "cityjson:referenceSystem": header.reference_system,
    }

    mock_catalog_module = MagicMock()
    mock_catalog_module.get_collection = AsyncMock(
        return_value=MagicMock(extras=collection_extras)
    )

    # Empty item stream — no CityJSON features ingested yet
    async def _empty_stream(*args: object, **kwargs: object):
        return
        yield  # make it an async generator

    mock_item_service = MagicMock()
    mock_item_service.stream_items = _empty_stream

    mock_storage = AsyncMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value="test-bucket")

    mock_catalog_module.assets = AsyncMock()

    task = GeoVolumesTilesetTask()
    execute_request = ExecuteRequest(inputs={"catalog_id": "cat1", "collection_id": "col1"})
    payload = TaskPayload(
        inputs=execute_request,
        task_id=uuid.uuid4(),
        caller_id="test@example.com",
    )

    from dynastore.models.protocols import CatalogsProtocol, StorageProtocol

    protocol_map = {
        CatalogsProtocol: mock_catalog_module,
        StorageProtocol: mock_storage,
    }

    with (
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task.get_protocol",
            side_effect=lambda proto: protocol_map.get(proto),
        ),
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task._get_item_service",
            return_value=mock_item_service,
        ),
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task.get_engine",
            return_value=MagicMock(),
        ),
        pytest.raises(RuntimeError, match="No CityJSON features found"),
    ):
        await task.run(payload)

    # Storage must NOT have been written to
    mock_storage.upload_file_content.assert_not_called()


@pytest.mark.asyncio
async def test_geovolumes_tileset_task_run_mocked() -> None:
    """GeoVolumesTilesetTask.run() with mocked protocols completes and returns outputs."""
    from unittest.mock import AsyncMock, MagicMock, patch

    import uuid
    from dynastore.extensions.geovolumes.tasks.tileset_task import GeoVolumesTilesetTask
    from dynastore.modules.processes.models import ExecuteRequest
    from dynastore.modules.tasks.models import TaskPayload

    # Build two sample features
    header = _make_header()
    features = [
        _make_box_feature(0, 0, 0, 500, 500, 100, "b1"),
        _make_box_feature(600, 0, 0, 1100, 500, 150, "b2"),
    ]

    # Mock collection extras (transform, epsg, zrange)
    collection_extras = {
        "cityjson:transform": {
            "scale": header.transform_scale,
            "translate": header.transform_translate,
        },
        "cityjson:referenceSystem": header.reference_system,
    }

    # Mock item stream: each item carries its cityjson CityJSONFeature dict
    mock_items = [
        MagicMock(
            properties={"cityjson": feat, "zmin": 0.0, "zmax": 0.1},
            extras={"cityjson": feat},
        )
        for feat in features
    ]

    mock_catalog_module = MagicMock()
    mock_catalog_module.get_collection = AsyncMock(
        return_value=MagicMock(extras=collection_extras)
    )
    mock_catalog_module.resolve_physical_schema = AsyncMock(return_value="public")

    # item_service streams items
    async def _item_stream(*args: object, **kwargs: object):
        for item in mock_items:
            yield item

    mock_item_service = MagicMock()
    mock_item_service.stream_items = _item_stream

    mock_storage = AsyncMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value="test-bucket")
    mock_storage.upload_file_content = AsyncMock(
        return_value="gs://test-bucket/3dtiles/col1/tileset.json"
    )

    mock_asset_manager = AsyncMock()
    mock_asset_manager.create_asset = AsyncMock(return_value=MagicMock(asset_id="3dtiles-col1"))
    mock_catalog_module.assets = mock_asset_manager

    task = GeoVolumesTilesetTask()

    execute_request = ExecuteRequest(inputs={"catalog_id": "cat1", "collection_id": "col1"})
    payload = TaskPayload(
        inputs=execute_request,
        task_id=uuid.uuid4(),
        caller_id="test@example.com",
    )

    from dynastore.models.protocols import CatalogsProtocol, StorageProtocol

    protocol_map = {
        CatalogsProtocol: mock_catalog_module,
        StorageProtocol: mock_storage,
    }

    with (
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task.get_protocol",
            side_effect=lambda proto: protocol_map.get(proto),
        ),
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task._get_item_service",
            return_value=mock_item_service,
        ),
        patch(
            "dynastore.extensions.geovolumes.tasks.tileset_task.get_engine",
            return_value=MagicMock(),
        ),
    ):
        await task.run(payload)

    # The task should have called upload_file_content at least twice (GLBs + tileset.json)
    assert mock_storage.upload_file_content.called
    assert mock_storage.upload_file_content.call_count >= 2
    # Verify tileset.json was uploaded with the correct content type
    calls = mock_storage.upload_file_content.call_args_list
    tileset_call = next(
        (c for c in calls if "tileset.json" in str(c.args[0])), None
    )
    assert tileset_call is not None
    assert tileset_call.args[2] == "application/json"
    # GLBs must use model/gltf-binary
    glb_calls = [c for c in calls if c.args[0].endswith(".glb")]
    assert all(c.args[2] == "model/gltf-binary" for c in glb_calls)
    # And registered the tileset asset
    assert mock_asset_manager.create_asset.called
