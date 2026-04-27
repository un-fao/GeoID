"""Unit tests for the PMTiles v3 writer."""

import io
import json
import struct

from dynastore.modules.tiles.writers.pmtiles_writer import (
    _serialize_directory,
    _write_varint,
    _Entry,
    write_pmtiles,
    zxy_to_tileid,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_header(data: bytes) -> dict:
    """Parse the 127-byte PMTiles v3 header into a dict."""
    assert data[:7] == b"PMTiles", "magic bytes mismatch"
    assert data[7] == 3, "version must be 3"
    h = {}
    h["root_dir_offset"] = struct.unpack_from("<Q", data, 8)[0]
    h["root_dir_length"] = struct.unpack_from("<Q", data, 16)[0]
    h["metadata_offset"] = struct.unpack_from("<Q", data, 24)[0]
    h["metadata_length"] = struct.unpack_from("<Q", data, 32)[0]
    h["leaf_dirs_offset"] = struct.unpack_from("<Q", data, 40)[0]
    h["leaf_dirs_length"] = struct.unpack_from("<Q", data, 48)[0]
    h["tile_data_offset"] = struct.unpack_from("<Q", data, 56)[0]
    h["tile_data_length"] = struct.unpack_from("<Q", data, 64)[0]
    h["n_addressed_tiles"] = struct.unpack_from("<Q", data, 72)[0]
    h["n_tile_entries"] = struct.unpack_from("<Q", data, 80)[0]
    h["n_tile_contents"] = struct.unpack_from("<Q", data, 88)[0]
    h["clustered"] = bool(data[96])
    h["internal_compression"] = data[97]
    h["tile_compression"] = data[98]
    h["tile_type"] = data[99]
    h["min_zoom"] = data[100]
    h["max_zoom"] = data[101]
    h["min_lon_e7"] = struct.unpack_from("<i", data, 102)[0]
    h["min_lat_e7"] = struct.unpack_from("<i", data, 106)[0]
    h["max_lon_e7"] = struct.unpack_from("<i", data, 110)[0]
    h["max_lat_e7"] = struct.unpack_from("<i", data, 114)[0]
    h["center_zoom"] = data[118]
    return h


# ---------------------------------------------------------------------------
# zxy_to_tileid
# ---------------------------------------------------------------------------

class TestZxyToTileId:
    def test_zoom_zero(self):
        assert zxy_to_tileid(0, 0, 0) == 0

    def test_zoom_one_known_values(self):
        # At zoom 1 there are 4 tiles (0/0/0, 0/1/0, 0/0/1, 0/1/1).
        # Base for zoom-1 = (4^1 - 1) // 3 = 1.
        base = 1
        ids = {(1, x, y): zxy_to_tileid(1, x, y) for x in range(2) for y in range(2)}
        # All IDs must be ≥ base and < base + 4
        for (z, x, y), tid in ids.items():
            assert base <= tid < base + 4, f"tile_id {tid} out of range for z={z} x={x} y={y}"
        # All IDs must be unique
        assert len(set(ids.values())) == 4

    def test_zoom_two_count(self):
        # 16 tiles at zoom 2; base = (4^2-1)//3 = 5
        base = 5
        ids = {zxy_to_tileid(2, x, y) for x in range(4) for y in range(4)}
        assert len(ids) == 16
        assert all(base <= t < base + 16 for t in ids)

    def test_deterministic(self):
        assert zxy_to_tileid(5, 10, 15) == zxy_to_tileid(5, 10, 15)

    def test_different_tiles_different_ids(self):
        assert zxy_to_tileid(3, 0, 0) != zxy_to_tileid(3, 1, 0)
        assert zxy_to_tileid(3, 0, 0) != zxy_to_tileid(3, 0, 1)


# ---------------------------------------------------------------------------
# _write_varint
# ---------------------------------------------------------------------------

class TestWriteVarint:
    def _encode(self, value: int) -> bytes:
        buf = bytearray()
        _write_varint(buf, value)
        return bytes(buf)

    def test_small_values(self):
        assert self._encode(0) == b"\x00"
        assert self._encode(1) == b"\x01"
        assert self._encode(127) == b"\x7f"

    def test_two_byte_values(self):
        # 128 = 0x80 → two bytes: 0x80, 0x01
        assert self._encode(128) == b"\x80\x01"
        assert self._encode(300) == b"\xac\x02"

    def test_large_value(self):
        encoded = self._encode(2**21 - 1)
        assert len(encoded) == 3


# ---------------------------------------------------------------------------
# _serialize_directory
# ---------------------------------------------------------------------------

class TestSerializeDirectory:
    def test_empty(self):
        data = _serialize_directory([])
        # First varint should be 0 (entry count)
        assert data[0] == 0

    def test_single_entry(self):
        entry = _Entry(tile_id=0, offset=0, length=100, run_length=1)
        data = _serialize_directory([entry])
        assert len(data) > 0

    def test_two_consecutive_entries_clustered_sentinel(self):
        e0 = _Entry(tile_id=0, offset=0, length=50, run_length=1)
        e1 = _Entry(tile_id=1, offset=50, length=60, run_length=1)
        # e1 is consecutive → its encoded offset should be 0 (sentinel)
        data = _serialize_directory([e0, e1])
        assert len(data) > 0

    def test_two_non_consecutive_entries(self):
        e0 = _Entry(tile_id=0, offset=0, length=50, run_length=1)
        e1 = _Entry(tile_id=5, offset=200, length=60, run_length=1)
        data = _serialize_directory([e0, e1])
        assert len(data) > 0


# ---------------------------------------------------------------------------
# write_pmtiles — full round-trip
# ---------------------------------------------------------------------------

class TestWritePMTiles:
    def _make_fake_tiles(self, count=4):
        """Return a list of (z, x, y, data) for the first *count* zoom-0/1 tiles."""
        tiles = [(0, 0, 0, b"\x00" * 10)]
        for x in range(2):
            for y in range(2):
                if len(tiles) >= count:
                    break
                tiles.append((1, x, y, b"\xff" * 20))
        return tiles[:count]

    def test_empty_input_produces_valid_archive(self):
        buf = io.BytesIO()
        stats = write_pmtiles([], buf)
        data = buf.getvalue()
        assert len(data) >= 127
        h = _parse_header(data)
        assert h["n_addressed_tiles"] == 0
        assert stats["n_tiles"] == 0
        assert stats["n_empty_tiles"] == 0

    def test_magic_and_version(self):
        buf = io.BytesIO()
        write_pmtiles([(0, 0, 0, b"abc")], buf)
        data = buf.getvalue()
        assert data[:7] == b"PMTiles"
        assert data[7] == 3

    def test_header_offsets_consistent(self):
        tiles = self._make_fake_tiles(4)
        buf = io.BytesIO()
        write_pmtiles(tiles, buf, min_zoom=0, max_zoom=1)
        data = buf.getvalue()
        h = _parse_header(data)

        # root dir immediately follows header
        assert h["root_dir_offset"] == 127
        # metadata follows root dir
        assert h["metadata_offset"] == 127 + h["root_dir_length"]
        # leaf dirs follow metadata
        assert h["leaf_dirs_offset"] == h["metadata_offset"] + h["metadata_length"]
        # tile data follows leaf dirs
        assert h["tile_data_offset"] == h["leaf_dirs_offset"] + h["leaf_dirs_length"]
        # total file length adds up
        expected_len = (
            h["tile_data_offset"] + h["tile_data_length"]
        )
        assert len(data) == expected_len

    def test_n_tiles_matches_non_empty_input(self):
        tiles = [(0, 0, 0, b"a"), (1, 0, 0, b"bb"), (1, 0, 1, b"")]  # one empty
        buf = io.BytesIO()
        stats = write_pmtiles(tiles, buf)
        assert stats["n_tiles"] == 2
        assert stats["n_empty_tiles"] == 1

    def test_metadata_written_as_json(self):
        buf = io.BytesIO()
        write_pmtiles(
            [(0, 0, 0, b"x")],
            buf,
            metadata={"name": "test", "format": "pbf"},
        )
        data = buf.getvalue()
        h = _parse_header(data)
        meta_bytes = data[h["metadata_offset"] : h["metadata_offset"] + h["metadata_length"]]
        # internal_compression: 1=NONE (built-in writer path), 2=GZIP
        # (protomaps `pmtiles` library path — its Writer.finalize gzip-wraps
        # the JSON metadata regardless of header tile_compression).
        if h["internal_compression"] == 2:
            import gzip
            meta_bytes = gzip.decompress(meta_bytes)
        meta = json.loads(meta_bytes.decode("utf-8"))
        assert meta["name"] == "test"
        assert meta["format"] == "pbf"

    def test_tile_type_mvt(self):
        buf = io.BytesIO()
        write_pmtiles([(0, 0, 0, b"x")], buf)
        data = buf.getvalue()
        h = _parse_header(data)
        assert h["tile_type"] == 1  # MVT

    def test_zoom_bounds_in_header(self):
        buf = io.BytesIO()
        write_pmtiles(
            [(2, 0, 0, b"x"), (3, 1, 1, b"y")],
            buf,
            min_zoom=2,
            max_zoom=3,
        )
        data = buf.getvalue()
        h = _parse_header(data)
        assert h["min_zoom"] == 2
        assert h["max_zoom"] == 3

    def test_bbox_encoded_in_header(self):
        bbox = (-10.0, -5.0, 10.0, 5.0)
        buf = io.BytesIO()
        write_pmtiles([(0, 0, 0, b"x")], buf, bbox=bbox)
        data = buf.getvalue()
        h = _parse_header(data)
        assert h["min_lon_e7"] == int(-10.0 * 1e7)
        assert h["min_lat_e7"] == int(-5.0 * 1e7)
        assert h["max_lon_e7"] == int(10.0 * 1e7)
        assert h["max_lat_e7"] == int(5.0 * 1e7)

    def test_total_bytes_stat(self):
        buf = io.BytesIO()
        stats = write_pmtiles([(0, 0, 0, b"hello")], buf)
        assert stats["total_bytes"] == len(buf.getvalue())

class TestWritePmtilesFromEntries:
    """Tests for the two-phase write_pmtiles_from_entries() helper."""

    def test_basic_roundtrip(self):
        """Entries + data source produces a valid PMTiles v3 archive."""
        from dynastore.modules.tiles.writers.pmtiles_writer import (
            write_pmtiles_from_entries, TileEntry, zxy_to_tileid,
        )
        tile_a = b"tile-data-a"
        tile_b = b"tile-data-b"

        data_src = io.BytesIO(tile_a + tile_b)
        entries = [
            TileEntry(tile_id=zxy_to_tileid(0, 0, 0), offset=0, length=len(tile_a)),
            TileEntry(tile_id=zxy_to_tileid(1, 0, 0), offset=len(tile_a), length=len(tile_b)),
        ]
        output = io.BytesIO()
        stats = write_pmtiles_from_entries(
            entries, data_src, output,
            metadata={}, min_zoom=0, max_zoom=1,
            bbox=(-180.0, -90.0, 180.0, 90.0),
        )

        data = output.getvalue()
        assert data[:7] == b"PMTiles", "magic bytes must be present"
        assert data[7] == 3, "version must be 3"
        assert stats["n_tiles"] == 2

    def test_empty_entries_produces_valid_archive(self):
        """Zero entries still produces a structurally valid archive."""
        from dynastore.modules.tiles.writers.pmtiles_writer import write_pmtiles_from_entries
        output = io.BytesIO()
        stats = write_pmtiles_from_entries(
            [], io.BytesIO(), output,
            metadata={}, min_zoom=0, max_zoom=0,
            bbox=(-180.0, -90.0, 180.0, 90.0),
        )
        data = output.getvalue()
        assert data[:7] == b"PMTiles"
        assert stats["n_tiles"] == 0

    def test_output_consistent_with_write_pmtiles(self):
        """write_pmtiles_from_entries and write_pmtiles produce archives with the same tile count."""
        from dynastore.modules.tiles.writers.pmtiles_writer import (
            write_pmtiles, write_pmtiles_from_entries, TileEntry, zxy_to_tileid,
        )
        tiles = [
            (0, 0, 0, b"mvt-0"),
            (1, 0, 0, b"mvt-1"),
            (1, 1, 0, b"mvt-2"),
        ]
        # write_pmtiles reference
        ref_buf = io.BytesIO()
        ref_stats = write_pmtiles(tiles, ref_buf)

        # write_pmtiles_from_entries path
        raw = b"".join(d for _, _, _, d in tiles)
        data_src = io.BytesIO(raw)
        offset = 0
        entries = []
        for z, x, y, d in tiles:
            entries.append(TileEntry(tile_id=zxy_to_tileid(z, x, y), offset=offset, length=len(d)))
            offset += len(d)
        entries.sort(key=lambda e: e.tile_id)

        out_buf = io.BytesIO()
        out_stats = write_pmtiles_from_entries(
            entries, data_src, out_buf,
            metadata={}, min_zoom=0, max_zoom=1,
            bbox=(-180.0, -90.0, 180.0, 90.0),
        )

        assert out_stats["n_tiles"] == ref_stats["n_tiles"] == len(tiles)
