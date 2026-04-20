import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    CollectionTypeEnum,
    CompositePartitionConfig,
)
from dynastore.modules.storage.driver_config import (
    CollectionDuckdbDriverConfig,
    CollectionIcebergDriverConfig,
    CollectionPostgresqlDriverConfig,
    DuckDBConfig,
    IcebergConfig,
)
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    AssetRoutingConfig,
    MetadataRoutingConfig,
    Operation,
    OperationDriverEntry,
    FailurePolicy,
    WriteMode,
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
# CollectionPostgresqlDriverConfig (sidecars, partitioning, collection_type)
# ---------------------------------------------------------------------------


class TestCollectionPostgresqlDriverConfigDefaults:
    def test_class_key(self):
        assert CollectionPostgresqlDriverConfig.class_key() == "CollectionPostgresqlDriverConfig"

    def test_default_sidecars(self):
        """M1b.1 keeps the eager `[geometries, attributes]` default.

        The default is removed in M1b.2 when the PG driver's
        ``_effective_sidecars(...)`` helper lands and resolves sidecar
        defaults lazily from the registry.  At that point this test
        asserts ``cfg.sidecars == []``; until then, the existing DDL /
        write / query call sites that iterate ``col_config.sidecars``
        directly stay green.
        """
        cfg = CollectionPostgresqlDriverConfig()
        assert len(cfg.sidecars) == 2

    def test_default_partitioning_disabled(self):
        cfg = CollectionPostgresqlDriverConfig()
        assert cfg.partitioning.enabled is False

    def test_default_collection_type(self):
        cfg = CollectionPostgresqlDriverConfig()
        assert cfg.collection_type == "VECTOR"

    def test_column_definitions(self):
        cfg = CollectionPostgresqlDriverConfig()
        cols = cfg.get_column_definitions()
        assert "geoid" in cols
        assert "transaction_time" in cols


class TestSidecarConfigDiscriminatorRoundTrip:
    """Round-trip tests for the M1b.1 discriminator-retention fix.

    Before M1b.1, ``SidecarConfig.sidecar_type`` was declared in the base
    class with no default; concrete subclasses supplied a
    ``Literal[...] = "..."`` default.  Pydantic treated that subclass
    default as not-explicitly-set on a default-constructed instance, so
    ``model_dump(exclude_unset=True)`` dropped the discriminator — making
    a round-trip through the ``Annotated[Union[...], Discriminator(...)]``
    on ``CollectionPostgresqlDriverConfig.sidecars`` fail.

    The fix is a ``@model_validator(mode="after")`` on ``SidecarConfig``
    that adds ``"sidecar_type"`` to ``__pydantic_fields_set__`` on every
    instance, regardless of how it was constructed.
    """

    def test_default_constructed_sidecar_dumps_keep_sidecar_type(self):
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
            ItemMetadataSidecarConfig,
        )
        for cls, expected in [
            (GeometriesSidecarConfig, "geometries"),
            (FeatureAttributeSidecarConfig, "attributes"),
            (ItemMetadataSidecarConfig, "item_metadata"),
        ]:
            dumped = cls().model_dump(exclude_unset=True)
            assert dumped.get("sidecar_type") == expected, (
                f"{cls.__name__} lost its sidecar_type under exclude_unset=True"
            )

    def test_round_trip_exclude_unset_preserves_discriminator(self):
        """model_dump(exclude_unset=True) → model_validate round-trips cleanly."""
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
            ItemMetadataSidecarConfig,
        )
        for cls in (GeometriesSidecarConfig, FeatureAttributeSidecarConfig, ItemMetadataSidecarConfig):
            original = cls()
            dumped = original.model_dump(exclude_unset=True)
            reloaded = cls.model_validate(dumped)
            assert reloaded.sidecar_type == original.sidecar_type
            assert type(reloaded) is cls

    def test_discriminated_union_dispatches_to_correct_subclass(self):
        """A default-body PG driver config dumps + reloads with sidecar types intact."""
        cfg = CollectionPostgresqlDriverConfig()
        dumped = cfg.model_dump(exclude_unset=False)  # full dump — simulates DB persistence
        reloaded = CollectionPostgresqlDriverConfig.model_validate(dumped)
        types_out = [s.sidecar_type for s in reloaded.sidecars]
        assert "geometries" in types_out
        assert "attributes" in types_out
        # Every reloaded entry is the specialised subclass (Union dispatched on sidecar_type)
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
        )
        subclass_map = {s.sidecar_type: type(s) for s in reloaded.sidecars}
        assert subclass_map.get("geometries") is GeometriesSidecarConfig
        assert subclass_map.get("attributes") is FeatureAttributeSidecarConfig


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
# CollectionRoutingConfig
# ---------------------------------------------------------------------------


class TestCollectionRoutingConfig:
    def test_class_key(self):
        assert CollectionRoutingConfig.class_key() == "CollectionRoutingConfig"

    def test_defaults(self):
        cfg = CollectionRoutingConfig()
        assert Operation.WRITE in cfg.operations
        assert Operation.READ in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "CollectionPostgresqlDriver"

    def test_custom_operations(self):
        cfg = CollectionRoutingConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="CollectionPostgresqlDriver")],
            Operation.READ: [OperationDriverEntry(driver_id="CollectionElasticsearchDriver", hints={"search"})],
            Operation.SEARCH: [OperationDriverEntry(driver_id="CollectionElasticsearchDriver", hints={"search"})],
        })
        assert len(cfg.operations) == 3
        assert cfg.operations[Operation.SEARCH][0].driver_id == "CollectionElasticsearchDriver"

    def test_failure_policy(self):
        entry = OperationDriverEntry(driver_id="es", on_failure=FailurePolicy.WARN)
        assert entry.on_failure == FailurePolicy.WARN

    def test_default_failure_policy_is_fatal(self):
        entry = OperationDriverEntry(driver_id="pg")
        assert entry.on_failure == FailurePolicy.FATAL


class TestOperationEnum:
    def test_transform_exists(self):
        assert Operation.TRANSFORM == "TRANSFORM"

    def test_all_operations(self):
        ops = {Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.TRANSFORM}
        assert len(ops) == 4


class TestWriteMode:
    def test_composition_modes_exist(self):
        assert WriteMode.FIRST == "first"
        assert WriteMode.FAN_OUT == "fan_out"
        assert WriteMode.CHAIN == "chain"

    def test_execution_modes_still_exist(self):
        assert WriteMode.SYNC == "sync"
        assert WriteMode.ASYNC == "async"


class TestMetadataRoutingConfig:
    def test_default_operations_empty(self):
        cfg = MetadataRoutingConfig()
        assert cfg.operations == {}

    def test_read_operation(self):
        cfg = MetadataRoutingConfig(operations={
            Operation.READ: [OperationDriverEntry(
                driver_id="MetadataElasticsearchDriver",
                write_mode=WriteMode.FIRST,
            )],
        })
        assert len(cfg.operations[Operation.READ]) == 1
        assert cfg.operations[Operation.READ][0].driver_id == "MetadataElasticsearchDriver"

    def test_transform_operation(self):
        cfg = MetadataRoutingConfig(operations={
            Operation.TRANSFORM: [OperationDriverEntry(
                driver_id="CollectionIcebergDriver",
                write_mode=WriteMode.CHAIN,
                on_failure=FailurePolicy.WARN,
            )],
        })
        entry = cfg.operations[Operation.TRANSFORM][0]
        assert entry.driver_id == "CollectionIcebergDriver"
        assert entry.write_mode == WriteMode.CHAIN
        assert entry.on_failure == FailurePolicy.WARN

    def test_read_and_transform_together(self):
        cfg = MetadataRoutingConfig(operations={
            Operation.READ: [OperationDriverEntry(driver_id="MetadataPostgresqlDriver")],
            Operation.TRANSFORM: [OperationDriverEntry(driver_id="CollectionIcebergDriver")],
        })
        assert Operation.READ in cfg.operations
        assert Operation.TRANSFORM in cfg.operations

    def test_embedded_in_collection_routing_config(self):
        """CollectionRoutingConfig.metadata must accept MetadataRoutingConfig."""
        cfg = CollectionRoutingConfig(metadata=MetadataRoutingConfig(operations={
            Operation.READ: [OperationDriverEntry(driver_id="MetadataPostgresqlDriver")],
        }))
        assert cfg.metadata.operations[Operation.READ][0].driver_id == "MetadataPostgresqlDriver"

    def test_default_embedded_metadata_is_empty(self):
        cfg = CollectionRoutingConfig()
        assert isinstance(cfg.metadata, MetadataRoutingConfig)
        assert cfg.metadata.operations == {}


class TestMetadataRoutingSnapshot:
    """Snapshot: metadata routing decisions are byte-identical before and after M5."""

    def _make_es_override_routing(self) -> CollectionRoutingConfig:
        """Equivalent of old metadata.override=[ES], storage=[]"""
        return CollectionRoutingConfig(
            metadata=MetadataRoutingConfig(operations={
                Operation.READ: [OperationDriverEntry(
                    driver_id="MetadataElasticsearchDriver",
                    write_mode=WriteMode.FIRST,
                )],
            })
        )

    def _make_iceberg_storage_routing(self) -> CollectionRoutingConfig:
        """Equivalent of old metadata.override=[], storage=[Iceberg]"""
        return CollectionRoutingConfig(
            metadata=MetadataRoutingConfig(operations={
                Operation.TRANSFORM: [OperationDriverEntry(
                    driver_id="CollectionIcebergDriver",
                    write_mode=WriteMode.CHAIN,
                    on_failure=FailurePolicy.WARN,
                )],
            })
        )

    def _make_mixed_routing(self) -> CollectionRoutingConfig:
        """Mixed: ES override + Iceberg transform."""
        return CollectionRoutingConfig(
            metadata=MetadataRoutingConfig(operations={
                Operation.READ: [OperationDriverEntry(
                    driver_id="MetadataElasticsearchDriver",
                )],
                Operation.TRANSFORM: [OperationDriverEntry(
                    driver_id="CollectionIcebergDriver",
                    on_failure=FailurePolicy.WARN,
                )],
            })
        )

    def test_read_driver_resolution(self):
        cfg = self._make_es_override_routing()
        read_entries = cfg.metadata.operations.get(Operation.READ, [])
        assert len(read_entries) == 1
        assert read_entries[0].driver_id == "MetadataElasticsearchDriver"

    def test_transform_driver_resolution(self):
        cfg = self._make_iceberg_storage_routing()
        transform_entries = cfg.metadata.operations.get(Operation.TRANSFORM, [])
        assert len(transform_entries) == 1
        assert transform_entries[0].driver_id == "CollectionIcebergDriver"

    def test_empty_read_returns_empty_list(self):
        cfg = self._make_iceberg_storage_routing()
        assert cfg.metadata.operations.get(Operation.READ, []) == []

    def test_empty_transform_returns_empty_list(self):
        cfg = self._make_es_override_routing()
        assert cfg.metadata.operations.get(Operation.TRANSFORM, []) == []

    def test_mixed_routing_both_ops(self):
        cfg = self._make_mixed_routing()
        assert len(cfg.metadata.operations[Operation.READ]) == 1
        assert len(cfg.metadata.operations[Operation.TRANSFORM]) == 1

    def test_failure_policy_preserved(self):
        cfg = self._make_mixed_routing()
        transform_entry = cfg.metadata.operations[Operation.TRANSFORM][0]
        assert transform_entry.on_failure == FailurePolicy.WARN


class TestAssetRoutingConfig:
    def test_class_key(self):
        assert AssetRoutingConfig.class_key() == "AssetRoutingConfig"

    def test_defaults(self):
        cfg = AssetRoutingConfig()
        assert Operation.WRITE in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "AssetPostgresqlDriver"


# ---------------------------------------------------------------------------
# M4 — env vs per-collection config split
# ---------------------------------------------------------------------------


class TestIcebergConfigEnvLevel:
    """IcebergConfig is an env-level singleton — connection fields live here."""

    def test_has_catalog_name(self):
        assert hasattr(IcebergConfig, "catalog_name")

    def test_has_catalog_type(self):
        assert hasattr(IcebergConfig, "catalog_type")
        assert IcebergConfig.catalog_type == "sql"

    def test_has_warehouse_uri(self):
        assert hasattr(IcebergConfig, "warehouse_uri")

    def test_has_catalog_uri(self):
        assert hasattr(IcebergConfig, "catalog_uri")

    def test_has_warehouse_scheme(self):
        assert hasattr(IcebergConfig, "warehouse_scheme")


class TestCollectionIcebergDriverConfigValidator:
    """CollectionIcebergDriverConfig rejects connection-level fields."""

    def test_valid_construction_no_fields(self):
        cfg = CollectionIcebergDriverConfig()
        assert cfg.namespace is None
        assert cfg.table_name is None

    def test_valid_construction_table_fields(self):
        cfg = CollectionIcebergDriverConfig(
            namespace="my_ns",
            table_name="my_table",
            table_properties={"write.format.default": "parquet"},
        )
        assert cfg.namespace == "my_ns"
        assert cfg.table_name == "my_table"
        assert cfg.table_properties == {"write.format.default": "parquet"}

    def test_valid_construction_partition_spec(self):
        spec = [{"name": "year", "transform": "year", "source": "event_date"}]
        cfg = CollectionIcebergDriverConfig(partition_spec=spec)
        assert cfg.partition_spec == spec

    def test_valid_construction_sort_order(self):
        order = [{"name": "id", "direction": "asc", "null_order": "nulls-last"}]
        cfg = CollectionIcebergDriverConfig(sort_order=order)
        assert cfg.sort_order == order

    def test_rejects_catalog_name(self):
        with pytest.raises(ValidationError, match="catalog_name"):
            CollectionIcebergDriverConfig(catalog_name="glue")

    def test_rejects_catalog_uri(self):
        with pytest.raises(ValidationError, match="catalog_uri"):
            CollectionIcebergDriverConfig(catalog_uri="http://catalog:8181")

    def test_rejects_catalog_type(self):
        with pytest.raises(ValidationError, match="catalog_type"):
            CollectionIcebergDriverConfig(catalog_type="rest")

    def test_rejects_catalog_properties(self):
        with pytest.raises(ValidationError, match="catalog_properties"):
            CollectionIcebergDriverConfig(catalog_properties={"key": "value"})

    def test_rejects_warehouse_uri(self):
        with pytest.raises(ValidationError, match="warehouse_uri"):
            CollectionIcebergDriverConfig(warehouse_uri="gs://my-bucket/iceberg/")

    def test_rejects_warehouse_scheme(self):
        with pytest.raises(ValidationError, match="warehouse_scheme"):
            CollectionIcebergDriverConfig(warehouse_scheme="gs")

    def test_rejects_multiple_connection_fields(self):
        with pytest.raises(ValidationError):
            CollectionIcebergDriverConfig(
                catalog_name="glue",
                warehouse_uri="s3://bucket/wh",
            )

    def test_class_key(self):
        assert CollectionIcebergDriverConfig.class_key() == "CollectionIcebergDriverConfig"


class TestDuckDBConfigEnvLevel:
    """DuckDBConfig has data_root for locating per-collection relative paths."""

    def test_has_data_root(self):
        assert hasattr(DuckDBConfig, "data_root")

    def test_data_root_is_string(self):
        assert isinstance(DuckDBConfig.data_root, str)


class TestCollectionDuckdbDriverConfigDefaults:
    def test_class_key(self):
        assert CollectionDuckdbDriverConfig.class_key() == "CollectionDuckdbDriverConfig"

    def test_default_format(self):
        cfg = CollectionDuckdbDriverConfig()
        assert cfg.format == "parquet"

    def test_path_fields_default_none(self):
        cfg = CollectionDuckdbDriverConfig()
        assert cfg.path is None
        assert cfg.write_path is None
