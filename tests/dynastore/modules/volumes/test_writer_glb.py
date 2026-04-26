"""Tests for modules/volumes/writers/glb.py."""

from __future__ import annotations

import json
import struct

import pytest

from dynastore.modules.volumes.mesh_builder import MeshBuffers, _empty_buffers
from dynastore.modules.volumes.writers.glb import (
    _CHUNK_BIN,
    _CHUNK_JSON,
    _GLB_MAGIC,
    _empty_glb,
    _pad4,
    pack_glb,
)


# ---------------------------------------------------------------------------
# _pad4
# ---------------------------------------------------------------------------


def test_pad4_already_aligned():
    data = b"abcd"
    assert _pad4(data) == data


def test_pad4_pads_with_spaces_by_default():
    data = b"ab"
    result = _pad4(data)
    assert len(result) == 4
    assert result[2:] == b"  "


def test_pad4_pads_with_zero_for_binary():
    data = b"abc"
    result = _pad4(data, pad_byte=0x00)
    assert len(result) == 4
    assert result[3] == 0x00


# ---------------------------------------------------------------------------
# _empty_glb
# ---------------------------------------------------------------------------


def _parse_glb_header(data: bytes):
    magic, version, length = struct.unpack_from("<III", data, 0)
    return magic, version, length


def test_empty_glb_valid_header():
    data = _empty_glb()
    magic, version, length = _parse_glb_header(data)
    assert magic == _GLB_MAGIC
    assert version == 2
    assert length == len(data)


def test_empty_glb_has_json_chunk():
    data = _empty_glb()
    chunk_len, chunk_type = struct.unpack_from("<II", data, 12)
    assert chunk_type == _CHUNK_JSON
    json_bytes = data[20:20 + chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert doc["asset"]["version"] == "2.0"


# ---------------------------------------------------------------------------
# pack_glb with no geometry
# ---------------------------------------------------------------------------


def test_pack_glb_empty_mesh_returns_valid_glb():
    buf = _empty_buffers()
    data = pack_glb(buf)
    magic, version, length = _parse_glb_header(data)
    assert magic == _GLB_MAGIC
    assert version == 2
    assert length == len(data)


# ---------------------------------------------------------------------------
# pack_glb with a minimal triangle
# ---------------------------------------------------------------------------


def _make_triangle_mesh() -> MeshBuffers:
    import struct as s
    verts = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (0.0, 1.0, 0.0)]
    pos = s.pack("<9f", *[v for xyz in verts for v in xyz])
    idx = s.pack("<3I", 0, 1, 2)
    return MeshBuffers(
        positions=pos,
        indices=idx,
        vertex_count=3,
        index_count=3,
        min_pos=(0.0, 0.0, 0.0),
        max_pos=(1.0, 1.0, 0.0),
    )


def test_pack_glb_triangle_header_length_matches():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    _, _, declared_length = _parse_glb_header(data)
    assert declared_length == len(data)


def test_pack_glb_triangle_has_json_and_bin_chunks():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)

    # JSON chunk
    json_chunk_len, json_type = struct.unpack_from("<II", data, 12)
    assert json_type == _CHUNK_JSON
    json_start = 20
    json_bytes = data[json_start:json_start + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert "accessors" in doc
    assert doc["accessors"][0]["count"] == 3  # 3 vertices

    # BIN chunk immediately after JSON chunk
    bin_offset = 12 + 8 + json_chunk_len
    bin_chunk_len, bin_type = struct.unpack_from("<II", data, bin_offset)
    assert bin_type == _CHUNK_BIN
    assert bin_chunk_len >= 3 * 3 * 4  # at least position data


def test_pack_glb_gltf_up_axis_z():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert doc["asset"].get("extras", {}).get("gltfUpAxis") == "Z"


def test_pack_glb_bufferviews_non_overlapping():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    bvs = doc["bufferViews"]
    assert bvs[0]["byteOffset"] == 0
    assert bvs[1]["byteOffset"] == bvs[0]["byteLength"]
