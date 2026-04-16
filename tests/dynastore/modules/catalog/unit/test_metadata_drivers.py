import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.drivers.metadata_postgresql import MetadataPostgresqlDriver
from dynastore.modules.elasticsearch.es_metadata_driver import MetadataElasticsearchDriver
from dynastore.models.protocols.metadata_driver import (
    CollectionMetadataStore,
    MetadataCapability,
)


class TestMetadataPostgresqlDriverProtocol:
    def test_is_protocol_instance(self):
        driver = MetadataPostgresqlDriver()
        assert isinstance(driver, CollectionMetadataStore)

    def test_driver_class_name(self):
        driver = MetadataPostgresqlDriver()
        assert type(driver).__name__ == "MetadataPostgresqlDriver"

    def test_capabilities(self):
        driver = MetadataPostgresqlDriver()
        assert MetadataCapability.READ in driver.capabilities
        assert MetadataCapability.WRITE in driver.capabilities
        assert MetadataCapability.SEARCH in driver.capabilities
        assert MetadataCapability.SOFT_DELETE in driver.capabilities
        assert MetadataCapability.SPATIAL_FILTER not in driver.capabilities

    def test_is_available_with_engine(self):
        driver = MetadataPostgresqlDriver()
        with patch.object(driver, "_get_engine", return_value=MagicMock()):
            import asyncio
            result = asyncio.get_event_loop().run_until_complete(driver.is_available())
            assert result is True

    def test_is_available_without_engine(self):
        driver = MetadataPostgresqlDriver()
        with patch.object(driver, "_get_engine", return_value=None):
            import asyncio
            result = asyncio.get_event_loop().run_until_complete(driver.is_available())
            assert result is False


class TestMetadataElasticsearchDriverProtocol:
    def test_is_protocol_instance(self):
        driver = MetadataElasticsearchDriver()
        assert isinstance(driver, CollectionMetadataStore)

    def test_driver_class_name(self):
        driver = MetadataElasticsearchDriver()
        assert type(driver).__name__ == "MetadataElasticsearchDriver"

    def test_capabilities(self):
        driver = MetadataElasticsearchDriver()
        assert MetadataCapability.READ in driver.capabilities
        assert MetadataCapability.WRITE in driver.capabilities
        assert MetadataCapability.SEARCH in driver.capabilities
        assert MetadataCapability.CQL_FILTER in driver.capabilities
        assert MetadataCapability.SPATIAL_FILTER in driver.capabilities
        assert MetadataCapability.AGGREGATION in driver.capabilities
        assert MetadataCapability.SOFT_DELETE not in driver.capabilities

    @pytest.mark.asyncio
    async def test_is_available_no_client(self):
        driver = MetadataElasticsearchDriver()
        with patch.object(driver, "_get_client", return_value=None):
            assert await driver.is_available() is False

    @pytest.mark.asyncio
    async def test_get_metadata_no_client(self):
        driver = MetadataElasticsearchDriver()
        with patch.object(driver, "_get_client", return_value=None):
            result = await driver.get_metadata("cat1", "col1")
            assert result is None


class TestMetadataPostgresqlDriverGetMetadata:
    @pytest.mark.asyncio
    async def test_get_metadata_returns_deserialized(self):
        driver = MetadataPostgresqlDriver()
        mock_engine = MagicMock()

        raw_row = {
            "collection_id": "col1",
            "title": '{"en": "Test Title"}',
            "description": '{"en": "Test Desc"}',
            "keywords": None,
            "license": None,
            "links": None,
            "assets": None,
            "extent": None,
            "providers": None,
            "summaries": None,
            "item_assets": None,
            "extra_metadata": None,
            "stac_extensions": None,
        }

        with patch.object(driver, "_get_engine", return_value=mock_engine), \
             patch("dynastore.modules.storage.drivers.metadata_postgresql.managed_transaction") as mock_tx, \
             patch.object(driver, "_resolve_physical_schema", return_value="schema1"), \
             patch("dynastore.modules.storage.drivers.metadata_postgresql.DQLQuery") as mock_dql:

            mock_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=raw_row)
            mock_dql.return_value = mock_query_instance

            result = await driver.get_metadata("cat1", "col1")
            assert result is not None
            assert result["title"] == {"en": "Test Title"}
            assert result["description"] == {"en": "Test Desc"}


class TestMetadataPostgresqlDriverSearchMetadata:
    @pytest.mark.asyncio
    async def test_search_metadata_no_engine(self):
        driver = MetadataPostgresqlDriver()
        with patch.object(driver, "_get_engine", return_value=None):
            results, total = await driver.search_metadata("cat1")
            assert results == []
            assert total == 0


class TestMetadataElasticsearchDriverBboxEnvelope:
    def test_bbox_to_envelope(self):
        from dynastore.modules.elasticsearch.es_metadata_driver import _bbox_to_envelope

        result = _bbox_to_envelope([-180, -90, 180, 90])
        assert result == {
            "type": "envelope",
            "coordinates": [[-180, 90], [180, -90]],
        }

    def test_bbox_to_envelope_empty(self):
        from dynastore.modules.elasticsearch.es_metadata_driver import _bbox_to_envelope

        assert _bbox_to_envelope([]) is None
        assert _bbox_to_envelope(None) is None

    def test_enrich_doc_adds_bbox_shape(self):
        driver = MetadataElasticsearchDriver()
        doc = {
            "id": "test",
            "extent": {
                "spatial": {
                    "bbox": [[-10, -20, 30, 40]],
                }
            }
        }
        enriched = driver._enrich_doc(doc)
        assert "bbox_shape" in enriched["extent"]["spatial"]
        assert enriched["extent"]["spatial"]["bbox_shape"]["type"] == "envelope"
