import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    CollectionTypeEnum,
    CompositePartitionConfig,
)
from dynastore.modules.storage.driver_config import (
    DriverRecordsPostgresqlConfig,
)
from dynastore.modules.storage.routing_config import (
    RoutingPluginConfig,
    AssetRoutingPluginConfig,
    Operation,
    OperationDriverEntry,
    FailurePolicy,
)


# ---------------------------------------------------------------------------
# CollectionPluginConfig (structural only — sidecars/partitioning moved to PG config)
# ---------------------------------------------------------------------------


class TestCollectionPluginConfigDefaults:
    def test_class_key(self):
        assert CollectionPluginConfig.class_key() == "CollectionPluginConfig"

    def test_extra_fields_allowed(self):
        cfg = CollectionPluginConfig(custom_field="value")
        assert cfg.custom_field == "value"


# ---------------------------------------------------------------------------
# DriverRecordsPostgresqlConfig (sidecars, partitioning, collection_type)
# ---------------------------------------------------------------------------


class TestDriverRecordsPostgresqlConfigDefaults:
    def test_class_key(self):
        assert DriverRecordsPostgresqlConfig.class_key() == "DriverRecordsPostgresqlConfig"

    def test_default_sidecars(self):
        cfg = DriverRecordsPostgresqlConfig()
        assert len(cfg.sidecars) == 2

    def test_default_partitioning_disabled(self):
        cfg = DriverRecordsPostgresqlConfig()
        assert cfg.partitioning.enabled is False

    def test_default_collection_type(self):
        cfg = DriverRecordsPostgresqlConfig()
        assert cfg.collection_type == "VECTOR"

    def test_column_definitions(self):
        cfg = DriverRecordsPostgresqlConfig()
        cols = cfg.get_column_definitions()
        assert "geoid" in cols
        assert "transaction_time" in cols


class TestCompositePartitionConfig:
    def test_disabled_no_keys(self):
        cfg = CompositePartitionConfig()
        assert cfg.enabled is False
        assert cfg.partition_keys == []

    def test_enabled_requires_keys(self):
        with pytest.raises(ValidationError):
            CompositePartitionConfig(enabled=True, partition_keys=[])

    def test_enabled_with_keys(self):
        cfg = CompositePartitionConfig(enabled=True, partition_keys=["asset_id"])
        assert cfg.partition_keys == ["asset_id"]


# ---------------------------------------------------------------------------
# RoutingPluginConfig
# ---------------------------------------------------------------------------


class TestRoutingPluginConfig:
    def test_class_key(self):
        assert RoutingPluginConfig.class_key() == "RoutingPluginConfig"

    def test_defaults(self):
        cfg = RoutingPluginConfig()
        assert Operation.WRITE in cfg.operations
        assert Operation.READ in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "DriverRecordsPostgresql"

    def test_custom_operations(self):
        cfg = RoutingPluginConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="DriverRecordsPostgresql")],
            Operation.READ: [OperationDriverEntry(driver_id="DriverRecordsElasticsearch", hints={"search"})],
            Operation.SEARCH: [OperationDriverEntry(driver_id="DriverRecordsElasticsearch", hints={"search"})],
        })
        assert len(cfg.operations) == 3
        assert cfg.operations[Operation.SEARCH][0].driver_id == "DriverRecordsElasticsearch"

    def test_failure_policy(self):
        entry = OperationDriverEntry(driver_id="es", on_failure=FailurePolicy.WARN)
        assert entry.on_failure == FailurePolicy.WARN

    def test_default_failure_policy_is_fatal(self):
        entry = OperationDriverEntry(driver_id="pg")
        assert entry.on_failure == FailurePolicy.FATAL


class TestAssetRoutingPluginConfig:
    def test_class_key(self):
        assert AssetRoutingPluginConfig.class_key() == "AssetRoutingPluginConfig"

    def test_defaults(self):
        cfg = AssetRoutingPluginConfig()
        assert Operation.WRITE in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "DriverAssetPostgresql"
