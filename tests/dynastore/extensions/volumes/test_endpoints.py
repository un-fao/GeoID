"""HTTP endpoint tests for the Volumes (3D GeoVolumes) service (Phase 5c)."""

from __future__ import annotations

import json

import pytest


def _build_service():
    from dynastore.extensions.volumes.volumes_service import VolumesService
    return VolumesService()


@pytest.mark.asyncio
async def test_tileset_json_returns_valid_skeleton():
    from unittest.mock import MagicMock

    from fastapi import Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    assert doc["asset"]["version"] == "1.0"
    assert "root" in doc
    assert resp.media_type == "application/json"


@pytest.mark.asyncio
async def test_tileset_json_template_points_at_tiles_subpath():
    """The empty-skeleton root has a [0]*12 box, but its content URI template
    (used once non-empty bounds arrive in Phase 5d) must already target
    the b3dm subpath."""
    from dynastore.extensions.volumes.config import VolumesConfig
    from dynastore.modules.volumes.bounds import FeatureBounds
    from dynastore.modules.volumes.tileset_builder import build_tileset

    # Drive the same template construction the handler uses.
    base = "http://ex/volumes/catalogs/c/collections/l/3dtiles"
    ts = build_tileset(
        [FeatureBounds("f", 0, 0, 0, 1, 1, 1)],
        VolumesConfig(),
        content_uri_template=f"{base}/tiles/{{tile_id}}.b3dm",
    )
    assert ts["root"]["content"]["uri"].startswith(base + "/tiles/")
    assert ts["root"]["content"]["uri"].endswith(".b3dm")


@pytest.mark.asyncio
async def test_b3dm_returns_501_pending_phase5b():
    from unittest.mock import MagicMock

    from fastapi import HTTPException, Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    with pytest.raises(HTTPException) as exc:
        await svc.get_tile_b3dm("c", "l", "0", request)
    assert exc.value.status_code == 501


@pytest.mark.asyncio
async def test_metadata_emits_self_and_data_links():
    from unittest.mock import MagicMock

    from fastapi import Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/metadata"

    payload = await svc.get_volumes_metadata("c", "l", request)
    assert payload["title"].startswith("3D GeoVolumes")
    rels = {lk["rel"] for lk in payload["links"]}
    assert rels == {"self", "data"}
    data_link = next(lk for lk in payload["links"] if lk["rel"] == "data")
    assert data_link["href"].endswith("/tileset.json")


@pytest.mark.asyncio
async def test_tileset_json_uses_registered_bounds_source(monkeypatch):
    """When a BoundsSourceProtocol is registered, its output is used."""
    import json
    from unittest.mock import MagicMock
    from fastapi import Request

    from dynastore.extensions.volumes.volumes_service import VolumesService
    from dynastore.modules.volumes.bounds import FeatureBounds
    import dynastore.extensions.volumes.volumes_service as mod

    class _Source:
        async def get_bounds(self, catalog_id, collection_id, *, limit=None):
            return [FeatureBounds("the-only-feature", 0, 0, 0, 1, 1, 1)]

    monkeypatch.setattr(mod, "get_protocol", lambda proto: _Source())

    svc = VolumesService()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    # Single feature -> root is a leaf with content URI pointing at the b3dm subpath.
    assert "content" in doc["root"]
    assert doc["root"]["content"]["uri"].endswith(".b3dm")


@pytest.mark.asyncio
async def test_tileset_json_falls_back_to_empty_source(monkeypatch):
    """No registered BoundsSourceProtocol -> EmptyBoundsSource -> skeleton."""
    import json
    from unittest.mock import MagicMock
    from fastapi import Request

    from dynastore.extensions.volumes.volumes_service import VolumesService
    import dynastore.extensions.volumes.volumes_service as mod

    monkeypatch.setattr(mod, "get_protocol", lambda proto: None)

    svc = VolumesService()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    # Empty bounds -> skeleton with [0]*12 box and no content URI.
    assert doc["root"]["boundingVolume"]["box"] == [0.0] * 12
    assert "content" not in doc["root"]
