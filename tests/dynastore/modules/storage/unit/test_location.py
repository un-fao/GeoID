import pytest

from dynastore.modules.storage.location import (
    StorageLocationConfig,
    StorageLocationConfigRegistry,
    PostgresStorageLocationConfig,
    FileStorageLocationConfig,
    OTFStorageLocationConfig,
)


class TestStorageLocationConfigRegistry:
    def test_resolve_postgresql(self):
        cls = StorageLocationConfigRegistry.resolve("postgresql")
        assert cls is PostgresStorageLocationConfig

    def test_resolve_duckdb(self):
        cls = StorageLocationConfigRegistry.resolve("duckdb")
        assert cls is FileStorageLocationConfig

    def test_resolve_iceberg(self):
        cls = StorageLocationConfigRegistry.resolve("iceberg")
        assert cls is OTFStorageLocationConfig

    def test_resolve_unknown_returns_base(self):
        cls = StorageLocationConfigRegistry.resolve("unknown_driver")
        assert cls is StorageLocationConfig


class TestPostgresStorageLocationConfig:
    def test_defaults(self):
        loc = PostgresStorageLocationConfig()
        assert loc.driver == "postgresql"
        assert loc.physical_schema is None
        assert loc.physical_table is None

    def test_with_overrides(self):
        loc = PostgresStorageLocationConfig(
            physical_schema="custom", physical_table="my_table"
        )
        assert loc.physical_schema == "custom"
        assert loc.physical_table == "my_table"

    def test_serialization_roundtrip(self):
        loc = PostgresStorageLocationConfig(physical_schema="s1", physical_table="t1")
        data = loc.model_dump()
        restored = PostgresStorageLocationConfig.model_validate(data)
        assert restored.physical_schema == "s1"
        assert restored.physical_table == "t1"


class TestFileStorageLocationConfig:
    def test_defaults(self):
        loc = FileStorageLocationConfig()
        assert loc.driver == "duckdb"
        assert loc.format == "parquet"
        assert loc.path is None
        assert loc.write_path is None

    def test_parquet_config(self):
        loc = FileStorageLocationConfig(
            format="parquet", path="/data/features.parquet"
        )
        assert loc.format == "parquet"
        assert loc.path == "/data/features.parquet"

    def test_csv_config(self):
        loc = FileStorageLocationConfig(format="csv", path="/data/features.csv")
        assert loc.format == "csv"

    def test_read_write_split(self):
        loc = FileStorageLocationConfig(
            format="parquet",
            path="/data/reads.parquet",
            write_path="/data/writes.db",
            write_format="sqlite",
        )
        assert loc.path == "/data/reads.parquet"
        assert loc.write_path == "/data/writes.db"
        assert loc.write_format == "sqlite"

    def test_serialization_roundtrip(self):
        loc = FileStorageLocationConfig(
            format="parquet", path="/data/f.parquet", uri="s3://bucket/f.parquet"
        )
        data = loc.model_dump()
        restored = FileStorageLocationConfig.model_validate(data)
        assert restored.format == "parquet"
        assert restored.path == "/data/f.parquet"
        assert restored.uri == "s3://bucket/f.parquet"


class TestOTFStorageLocationConfig:
    def test_defaults(self):
        loc = OTFStorageLocationConfig()
        assert loc.driver == "iceberg"
        assert loc.catalog_name is None
        assert loc.namespace is None
        assert loc.table_name is None

    def test_full_config(self):
        loc = OTFStorageLocationConfig(
            catalog_name="glue",
            catalog_uri="https://glue.us-east-1.amazonaws.com",
            namespace="geospatial",
            table_name="features",
        )
        assert loc.catalog_name == "glue"
        assert loc.namespace == "geospatial"
        assert loc.table_name == "features"

    def test_serialization_roundtrip(self):
        loc = OTFStorageLocationConfig(
            catalog_name="hive", namespace="ns", table_name="tbl"
        )
        data = loc.model_dump()
        restored = OTFStorageLocationConfig.model_validate(data)
        assert restored.catalog_name == "hive"
        assert restored.table_name == "tbl"


class TestRegistryDeserialization:
    def test_deserialize_via_registry(self):
        raw = {"driver": "postgresql", "physical_schema": "my_schema"}
        cls = StorageLocationConfigRegistry.resolve("postgresql")
        loc = cls.model_validate(raw)
        assert isinstance(loc, PostgresStorageLocationConfig)
        assert loc.physical_schema == "my_schema"

    def test_deserialize_unknown_driver(self):
        raw = {"driver": "custom", "uri": "s3://bucket/path"}
        cls = StorageLocationConfigRegistry.resolve("custom")
        loc = cls.model_validate(raw)
        assert isinstance(loc, StorageLocationConfig)
        assert loc.uri == "s3://bucket/path"
