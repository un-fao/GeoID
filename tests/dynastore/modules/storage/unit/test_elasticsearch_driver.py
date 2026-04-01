import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.drivers.elasticsearch import (
    ElasticsearchStorageDriver,
    ElasticsearchObfuscatedDriver,
    _ElasticsearchBase,
)
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError


class TestElasticsearchBase:
    def test_sfeos_available_returns_bool(self):
        result = _ElasticsearchBase._sfeos_available()
        assert isinstance(result, bool)

    def test_normalize_entities_single_feature(self):
        feature = MagicMock(spec=Feature)
        result = _ElasticsearchBase._normalize_entities(feature)
        assert result == [feature]

    def test_normalize_entities_feature_collection(self):
        fc = MagicMock(spec=FeatureCollection)
        fc.features = [MagicMock(spec=Feature), MagicMock(spec=Feature)]
        result = _ElasticsearchBase._normalize_entities(fc)
        assert len(result) == 2

    def test_normalize_entities_feature_collection_empty(self):
        fc = MagicMock(spec=FeatureCollection)
        fc.features = None
        result = _ElasticsearchBase._normalize_entities(fc)
        assert result == []

    def test_normalize_entities_list(self):
        items = [{"id": "a"}, {"id": "b"}]
        result = _ElasticsearchBase._normalize_entities(items)
        assert result == items

    def test_normalize_entities_dict(self):
        item = {"id": "a"}
        result = _ElasticsearchBase._normalize_entities(item)
        assert result == [item]

    def test_extract_item_id_from_feature(self):
        feature = MagicMock()
        feature.id = "test-id"
        assert _ElasticsearchBase._extract_item_id(feature) == "test-id"

    def test_extract_item_id_from_dict(self):
        assert _ElasticsearchBase._extract_item_id({"id": "test-id"}) == "test-id"

    def test_extract_item_id_none_for_missing(self):
        assert _ElasticsearchBase._extract_item_id({}) is None

    def test_feature_to_stac_item_from_pydantic(self):
        feature = MagicMock()
        feature.model_dump.return_value = {"id": "f1", "type": "Feature"}
        result = _ElasticsearchBase._feature_to_stac_item(feature, "cat1", "col1")
        assert result["collection"] == "col1"
        assert result["id"] == "f1"

    def test_feature_to_stac_item_from_dict(self):
        result = _ElasticsearchBase._feature_to_stac_item(
            {"id": "f1", "type": "Feature"}, "cat1", "col1"
        )
        assert result["collection"] == "col1"
        assert result["id"] == "f1"


class TestIsSecondaryFor:
    @pytest.mark.asyncio
    async def test_returns_true_when_listed(self):
        from dynastore.modules.storage.routing_config import (
            RoutingPluginConfig, Operation, OperationDriverEntry,
        )

        mock_configs = AsyncMock()
        routing = RoutingPluginConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="postgresql")],
            Operation.READ: [OperationDriverEntry(driver_id="elasticsearch")],
        })
        mock_configs.get_config = AsyncMock(return_value=routing)

        with patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_listed(self):
        from dynastore.modules.storage.routing_config import (
            RoutingPluginConfig, Operation, OperationDriverEntry,
        )

        mock_configs = AsyncMock()
        routing = RoutingPluginConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="postgresql")],
            Operation.READ: [OperationDriverEntry(driver_id="duckdb")],
        })
        mock_configs.get_config = AsyncMock(return_value=routing)

        with patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self):
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=Exception("boom"),
        ):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_configs(self):
        with patch("dynastore.tools.discovery.get_protocol", return_value=None):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False


class TestElasticsearchStorageDriverMeta:
    def test_driver_id(self):
        driver = ElasticsearchStorageDriver()
        assert driver.driver_id == "elasticsearch"

    def test_priority(self):
        driver = ElasticsearchStorageDriver()
        assert driver.priority == 50

    def test_capabilities(self):
        driver = ElasticsearchStorageDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SPATIAL_FILTER in driver.capabilities
        assert Capability.FULLTEXT in driver.capabilities
        assert Capability.SOFT_DELETE in driver.capabilities

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ElasticsearchStorageDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")


class TestElasticsearchObfuscatedDriverMeta:
    def test_driver_id(self):
        driver = ElasticsearchObfuscatedDriver()
        assert driver.driver_id == "elasticsearch_obfuscated"

    def test_priority(self):
        driver = ElasticsearchObfuscatedDriver()
        assert driver.priority == 51

    def test_capabilities(self):
        driver = ElasticsearchObfuscatedDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.FULLTEXT not in driver.capabilities
        assert Capability.SOFT_DELETE not in driver.capabilities

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ElasticsearchObfuscatedDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        driver = ElasticsearchObfuscatedDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)

    @pytest.mark.asyncio
    async def test_soft_drop_raises(self):
        driver = ElasticsearchObfuscatedDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.drop_storage("cat1", "col1", soft=True)


class TestQueryRequestToEs:
    def test_empty_request(self):
        from dynastore.models.query_builder import QueryRequest
        request = QueryRequest()
        result = ElasticsearchStorageDriver._query_request_to_es(request)
        assert result == {"query": {"match_all": {}}}

    def test_eq_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[FilterCondition(field="status", operator="eq", value="active")]
        )
        result = ElasticsearchStorageDriver._query_request_to_es(request)
        assert result["query"]["bool"]["must"][0] == {"term": {"status": "active"}}

    def test_bbox_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[
                FilterCondition(
                    field="geometry",
                    operator="bbox",
                    value=[10.0, 40.0, 15.0, 45.0],
                )
            ]
        )
        result = ElasticsearchStorageDriver._query_request_to_es(request)
        geo_filter = result["query"]["bool"]["must"][0]
        assert "geo_bounding_box" in geo_filter

    def test_like_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[FilterCondition(field="name", operator="like", value="test*")]
        )
        result = ElasticsearchStorageDriver._query_request_to_es(request)
        assert result["query"]["bool"]["must"][0] == {"wildcard": {"name": "test*"}}

    def test_multiple_filters(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[
                FilterCondition(field="status", operator="eq", value="active"),
                FilterCondition(field="name", operator="like", value="test*"),
            ]
        )
        result = ElasticsearchStorageDriver._query_request_to_es(request)
        must = result["query"]["bool"]["must"]
        assert len(must) == 2
