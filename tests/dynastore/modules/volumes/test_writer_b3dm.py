"""Tests for modules/volumes/writers/b3dm.py."""

from __future__ import annotations

import json
import struct

from dynastore.modules.volumes.writers.b3dm import (
    _B3DM_MAGIC,
    _HEADER_SIZE,
    _pad8,
    pack_b3dm,
)


# ---------------------------------------------------------------------------
# _pad8
# ---------------------------------------------------------------------------


def test_pad8_already_aligned():
    data = b"abcdefgh"
    assert _pad8(data) == data


def test_pad8_pads_to_eight():
    data = b"abc"
    result = _pad8(data)
    assert len(result) == 8
    assert result[3:] == b"     " * 1  # spaces (default pad_byte=0x20)


# ---------------------------------------------------------------------------
# Header structure
# ---------------------------------------------------------------------------


def _parse_header(data: bytes):
    magic, version, byte_len, ft_json_len, ft_bin_len, bt_json_len, bt_bin_len = (
        struct.unpack_from("<4sIIIIII", data, 0)
    )
    return {
        "magic": magic,
        "version": version,
        "byteLength": byte_len,
        "featureTableJSONByteLength": ft_json_len,
        "featureTableBinaryByteLength": ft_bin_len,
        "batchTableJSONByteLength": bt_json_len,
        "batchTableBinaryByteLength": bt_bin_len,
    }


def test_pack_b3dm_magic_and_version():
    data = pack_b3dm(b"fake_glb")
    hdr = _parse_header(data)
    assert hdr["magic"] == _B3DM_MAGIC
    assert hdr["version"] == 1


def test_pack_b3dm_total_length_matches():
    glb = b"\x01" * 64
    data = pack_b3dm(glb)
    hdr = _parse_header(data)
    assert hdr["byteLength"] == len(data)


def test_pack_b3dm_feature_table_has_batch_length():
    data = pack_b3dm(b"glb", feature_ids=["a", "b", "c"])
    hdr = _parse_header(data)
    ft_start = _HEADER_SIZE
    ft_json_bytes = data[ft_start:ft_start + hdr["featureTableJSONByteLength"]]
    ft = json.loads(ft_json_bytes.decode("utf-8").strip())
    assert ft["BATCH_LENGTH"] == 3


def test_pack_b3dm_no_feature_ids_batch_length_zero():
    data = pack_b3dm(b"glb")
    hdr = _parse_header(data)
    ft_start = _HEADER_SIZE
    ft_json_bytes = data[ft_start:ft_start + hdr["featureTableJSONByteLength"]]
    ft = json.loads(ft_json_bytes.decode("utf-8").strip())
    assert ft["BATCH_LENGTH"] == 0
    assert hdr["batchTableJSONByteLength"] == 0


def test_pack_b3dm_batch_table_contains_ids():
    data = pack_b3dm(b"glb", feature_ids=["id1", "id2"])
    hdr = _parse_header(data)
    bt_start = (
        _HEADER_SIZE
        + hdr["featureTableJSONByteLength"]
        + hdr["featureTableBinaryByteLength"]
    )
    bt_json_bytes = data[bt_start:bt_start + hdr["batchTableJSONByteLength"]]
    bt = json.loads(bt_json_bytes.decode("utf-8").strip())
    assert bt["id"] == ["id1", "id2"]


def test_pack_b3dm_glb_appended_at_end():
    glb_payload = b"GLB_CONTENT_HERE"
    data = pack_b3dm(glb_payload, feature_ids=["x"])
    hdr = _parse_header(data)
    glb_start = (
        _HEADER_SIZE
        + hdr["featureTableJSONByteLength"]
        + hdr["featureTableBinaryByteLength"]
        + hdr["batchTableJSONByteLength"]
        + hdr["batchTableBinaryByteLength"]
    )
    assert data[glb_start:] == glb_payload


def test_pack_b3dm_feature_table_json_8byte_aligned():
    data = pack_b3dm(b"glb", feature_ids=["a"])
    hdr = _parse_header(data)
    assert hdr["featureTableJSONByteLength"] % 8 == 0
