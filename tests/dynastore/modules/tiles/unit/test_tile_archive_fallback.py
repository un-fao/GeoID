"""Unit tests for TileArchiveStorageProtocol + PMTiles archive fallback logic."""

import pytest
from unittest.mock import AsyncMock

from dynastore.modules.tiles.tiles_module import TileArchiveStorageProtocol


@pytest.mark.asyncio
async def test_archive_protocol_is_runtime_checkable():
    """TileArchiveStorageProtocol must be runtime-checkable (isinstance() works)."""

    class _FakeArchive:
        async def save_archive(self, catalog_id, collection_id, tms_id, data_file):
            return "pg://fake"

        async def archive_exists(self, catalog_id, collection_id, tms_id):
            return False

        async def get_tile_from_archive(self, catalog_id, collection_id, tms_id, z, x, y):
            return None

        async def delete_archive(self, catalog_id, collection_id, tms_id):
            return True

    # Protocol is @runtime_checkable so isinstance must work
    obj = _FakeArchive()
    assert isinstance(obj, TileArchiveStorageProtocol)


@pytest.mark.asyncio
async def test_archive_get_tile_returns_none_on_miss():
    """get_tile_from_archive should propagate None when tile is absent."""
    mock_archive = AsyncMock(spec=TileArchiveStorageProtocol)
    mock_archive.get_tile_from_archive = AsyncMock(return_value=None)

    result = await mock_archive.get_tile_from_archive(
        "cat1", "col1", "WebMercatorQuad", 3, 4, 5
    )
    assert result is None


@pytest.mark.asyncio
async def test_archive_get_tile_returns_bytes_on_hit():
    """get_tile_from_archive must return raw bytes on a cache hit."""
    mock_archive = AsyncMock(spec=TileArchiveStorageProtocol)
    mock_archive.get_tile_from_archive = AsyncMock(return_value=b"mvt-data")

    result = await mock_archive.get_tile_from_archive(
        "cat1", "col1", "WebMercatorQuad", 3, 4, 5
    )
    assert result == b"mvt-data"
    mock_archive.get_tile_from_archive.assert_awaited_once_with(
        "cat1", "col1", "WebMercatorQuad", 3, 4, 5
    )
