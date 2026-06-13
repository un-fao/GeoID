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

import asyncio
import json
import pytest
from pathlib import Path
from pydantic import BaseModel
from typing import Optional, Dict, Any

from dynastore.extensions.tools.formatters import (
    format_response,
    OutputFormatEnum,
    _parse_srid_from_srs_name,
)


# --- Fixtures ---


@pytest.fixture
def test_data():
    """Load test data from JSON fixture."""
    data_path = Path(__file__).parent / "data" / "formatters_test_data.json"
    with open(data_path) as f:
        return json.load(f)


@pytest.fixture
def sample_features(test_data):
    """Sample GeoJSON features as dicts."""
    return test_data["sample_features"]


@pytest.fixture
def empty_features(test_data):
    """Empty feature list."""
    return test_data["empty_features"]


class MockFeatureModel(BaseModel):
    """Mock Pydantic model to test model_dump handling."""

    type: str = "Feature"
    id: str
    geometry: Optional[Dict[str, Any]] = None
    properties: Dict[str, Any] = {}


# --- Tests for format_response ---


class TestFormatResponse:
    """Tests for the format_response function."""

    def test_format_response_csv_with_dicts(self, sample_features):
        """Test format_response with CSV format using plain dicts."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.CSV,
        )

        assert response.media_type == "text/csv"
        assert 'filename="layer.csv"' in response.headers.get("content-disposition", "")

    def test_format_response_csv_with_pydantic_models(self):
        """Test format_response with CSV format using Pydantic models."""
        models = [
            MockFeatureModel(id="1", properties={"name": "Test1"}),
            MockFeatureModel(id="2", properties={"name": "Test2"}),
        ]

        response = format_response(
            features=iter(models),
            output_format=OutputFormatEnum.CSV,
        )

        assert response.media_type == "text/csv"

    def test_format_response_custom_collection_id(self, sample_features):
        """Test format_response with custom collection_id."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.CSV,
            collection_id="my_custom_layer",
        )

        assert 'filename="my_custom_layer.csv"' in response.headers.get(
            "content-disposition", ""
        )

    def test_format_response_custom_target_srid(self, sample_features):
        """Test format_response with custom target_srid."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.CSV,
            target_srid=3857,
        )

        # Response should be created successfully with custom SRID
        assert response is not None
        assert response.media_type == "text/csv"

    def test_format_response_geopackage(self, sample_features):
        """Test format_response with GeoPackage format."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.GEOPACKAGE,
        )

        assert response.media_type == "application/geopackage+sqlite3"
        assert 'filename="layer.gpkg"' in response.headers.get(
            "content-disposition", ""
        )

    def test_format_response_shapefile(self, sample_features):
        """Test format_response with Shapefile format."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.SHAPEFILE,
        )

        assert response.media_type == "application/zip"
        assert 'filename="layer.zip"' in response.headers.get("content-disposition", "")

    def test_format_response_parquet(self, sample_features):
        """Test format_response with Parquet format."""
        response = format_response(
            features=iter(sample_features),
            output_format=OutputFormatEnum.PARQUET,
        )

        assert response.media_type == "application/octet-stream"
        assert 'filename="layer.parquet"' in response.headers.get(
            "content-disposition", ""
        )

    def test_format_response_unsupported_format(self, sample_features):
        """Test format_response raises error for unsupported formats."""
        # Create a dummy enum value or just pass a string that isn't in the map
        # But format_response expects OutputFormatEnum.
        # Let's mock the enum or use a raw string if not strictly typed at runtime?
        # Actually, python enums are strict.

        # We need an enum member that is NOT in format_map.
        # If all members are in format_map, we can't test this easily without mocking format_map.

        from unittest.mock import patch

        # Patch format_map to ensure our key is missing
        with patch("dynastore.extensions.tools.formatters.format_map", {}):
            with pytest.raises(ValueError, match="Unsupported format"):
                format_response(
                    features=iter(sample_features),
                    output_format=OutputFormatEnum.CSV,  # Should fail because map is empty
                )

    def test_format_response_empty_features(self, empty_features):
        """Test format_response handles empty feature list."""
        response = format_response(
            features=iter(empty_features),
            output_format=OutputFormatEnum.CSV,
        )

        assert response is not None
        assert response.media_type == "text/csv"


# --- Tests for _parse_srid_from_srs_name ---


class TestParseSridFromSrsName:
    """Tests for the _parse_srid_from_srs_name helper function."""

    def test_parse_urn_format(self):
        """Test parsing URN-style srsName."""
        assert _parse_srid_from_srs_name("urn:ogc:def:crs:EPSG::4326") == 4326
        assert _parse_srid_from_srs_name("urn:ogc:def:crs:EPSG::3857") == 3857

    def test_parse_none_input(self):
        """Test parsing None input returns None."""
        assert _parse_srid_from_srs_name(None) is None

    def test_parse_empty_string(self):
        """Test parsing empty string returns None."""
        assert _parse_srid_from_srs_name("") is None

    def test_parse_invalid_format(self):
        """Test parsing invalid format returns None."""
        assert _parse_srid_from_srs_name("invalid") is None
        assert _parse_srid_from_srs_name("EPSG:4326") is None  # Missing double colon


# --- Async-source regression: cross-event-loop drain for file formats ---


class TestFormatResponseAsyncSource:
    """Non-JSON file formats must drain an ASYNC feature source on the request
    loop, never on a freshly-created loop.

    The DWH-join ``shp`` export streams items from an asyncpg/Elasticsearch async
    iterator. Those drivers are bound to the request event loop. The file-format
    branch of ``format_response`` hands a *sync* generator to Starlette, which
    drives it in a worker thread; the previous implementation spun up a brand-new
    ``asyncio.new_event_loop()`` there and re-drove the async source on it, which
    raised "got Future attached to a different loop" /
    "Timeout context manager should be used inside a task" the moment a real
    asyncpg/aiohttp object was touched. These tests pin the invariant: the source
    is consumed on the *running* loop, and valid bytes come out.
    """

    async def test_shapefile_async_source_consumed_on_request_loop(self):
        request_loop = asyncio.get_running_loop()
        consumed_on: list = []

        async def async_features():
            # Touch the running loop so we can prove which loop drained us; in
            # production this is where asyncpg/aiohttp I/O happens.
            await asyncio.sleep(0)
            consumed_on.append(asyncio.get_running_loop())
            yield {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.49, 41.90]},
                "properties": {"name": "Rome"},
            }

        response = format_response(
            features=async_features(),
            output_format=OutputFormatEnum.SHAPEFILE,
            collection_id="region",
        )

        # Consume the body exactly as Starlette does (sync writer -> threadpool).
        body = b"".join([chunk async for chunk in response.body_iterator])

        assert consumed_on, "async source was never consumed"
        assert consumed_on[0] is request_loop, (
            "async feature source must be drained on the request loop, "
            f"not a foreign loop {consumed_on[0]!r}"
        )

        import io
        import zipfile

        assert body, "shapefile body must not be empty"
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            names = zf.namelist()
        assert any(n.endswith(".shp") for n in names), names

    async def test_csv_async_source_consumed_on_request_loop(self):
        request_loop = asyncio.get_running_loop()
        consumed_on: list = []

        async def async_features():
            await asyncio.sleep(0)
            consumed_on.append(asyncio.get_running_loop())
            yield {
                "type": "Feature",
                "geometry": None,
                "properties": {"name": "Rome", "pop": 2},
            }

        response = format_response(
            features=async_features(),
            output_format=OutputFormatEnum.CSV,
            collection_id="region",
        )

        body = b"".join([chunk async for chunk in response.body_iterator])

        assert consumed_on and consumed_on[0] is request_loop
        assert b"Rome" in body
