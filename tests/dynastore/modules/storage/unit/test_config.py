import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    COLLECTION_PLUGIN_CONFIG_ID,
)
from dynastore.modules.storage.config import DriverRef
from dynastore.modules.storage.location import (
    FileStorageLocationConfig,
    PostgresStorageLocationConfig,
    StorageLocationConfig,
)


class TestDriverRef:
    def test_create(self):
        ref = DriverRef(driver_id="postgresql")
        assert ref.driver_id == "postgresql"

    def test_str(self):
        ref = DriverRef(driver_id="postgresql")
        assert str(ref) == "postgresql"

    def test_hash(self):
        ref = DriverRef(driver_id="postgresql")
        assert hash(ref) == hash("postgresql")

    def test_frozen(self):
        ref = DriverRef(driver_id="postgresql")
        with pytest.raises(ValidationError):
            ref.driver_id = "elasticsearch"

    def test_strips_whitespace(self):
        ref = DriverRef(driver_id="  elasticsearch  ")
        assert ref.driver_id == "elasticsearch"

    def test_empty_rejected(self):
        with pytest.raises(ValidationError):
            DriverRef(driver_id="")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValidationError):
            DriverRef(driver_id="   ")


class TestCollectionPluginConfigDefaults:
    def test_default_write_driver(self):
        cfg = CollectionPluginConfig()
        assert cfg.write_driver_id == "postgresql"

    def test_default_read_drivers_empty(self):
        cfg = CollectionPluginConfig()
        assert cfg.read_drivers == {}

    def test_default_secondary_drivers_empty(self):
        cfg = CollectionPluginConfig()
        assert cfg.secondary_drivers == []

    def test_plugin_id(self):
        assert CollectionPluginConfig._plugin_id == COLLECTION_PLUGIN_CONFIG_ID
        assert COLLECTION_PLUGIN_CONFIG_ID == "collection"


class TestPrimaryDriverValidation:
    def test_valid_write_driver(self):
        cfg = CollectionPluginConfig(write_driver="elasticsearch")
        assert cfg.write_driver_id == "elasticsearch"

    def test_legacy_primary_driver_alias(self):
        cfg = CollectionPluginConfig(primary_driver="elasticsearch")
        assert cfg.write_driver_id == "elasticsearch"

    def test_strips_whitespace(self):
        cfg = CollectionPluginConfig(write_driver="  elasticsearch  ")
        assert cfg.write_driver_id == "elasticsearch"

    def test_empty_string_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(write_driver="")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(write_driver="   ")

    def test_accepts_driver_ref(self):
        cfg = CollectionPluginConfig(write_driver=DriverRef(driver_id="iceberg"))
        assert cfg.write_driver_id == "iceberg"


class TestReadDriversValidation:
    def test_valid_read_drivers(self):
        cfg = CollectionPluginConfig(read_drivers={"search": "elasticsearch"})
        assert cfg.resolve_read_driver_id("search") == "elasticsearch"

    def test_multiple_hints(self):
        cfg = CollectionPluginConfig(
            read_drivers={
                "search": "elasticsearch",
                "analytics": "duckdb",
                "default": "postgresql",
            }
        )
        assert len(cfg.read_drivers) == 3

    def test_empty_driver_value_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(read_drivers={"search": ""})

    def test_whitespace_driver_value_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(read_drivers={"search": "   "})


class TestSecondaryDriversValidation:
    def test_valid_secondary_drivers(self):
        cfg = CollectionPluginConfig(secondary_drivers=["elasticsearch"])
        assert cfg.secondary_driver_ids == ["elasticsearch"]

    def test_multiple_secondaries(self):
        cfg = CollectionPluginConfig(
            secondary_drivers=["elasticsearch", "elasticsearch_obfuscated"]
        )
        assert len(cfg.secondary_drivers) == 2
        assert cfg.secondary_driver_ids == ["elasticsearch", "elasticsearch_obfuscated"]

    def test_empty_entry_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(secondary_drivers=[""])

    def test_whitespace_entry_rejected(self):
        with pytest.raises(ValidationError):
            CollectionPluginConfig(secondary_drivers=["   "])


class TestResolveReadDriverId:
    def test_hint_match(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"search": "elasticsearch"},
        )
        assert cfg.resolve_read_driver_id("search") == "elasticsearch"

    def test_falls_back_to_default(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"default": "duckdb"},
        )
        assert cfg.resolve_read_driver_id("analytics") == "duckdb"

    def test_falls_back_to_write_driver(self):
        cfg = CollectionPluginConfig(write_driver="postgresql")
        assert cfg.resolve_read_driver_id("search") == "postgresql"


class TestAvailableHints:
    def test_returns_keys(self):
        cfg = CollectionPluginConfig(
            read_drivers={"search": "elasticsearch", "analytics": "duckdb"}
        )
        assert cfg.available_hints() == {"search", "analytics"}

    def test_empty(self):
        cfg = CollectionPluginConfig()
        assert cfg.available_hints() == set()


class TestStorageLocations:
    def test_get_location(self):
        cfg = CollectionPluginConfig(
            storage_locations={
                "duckdb": FileStorageLocationConfig(format="parquet", path="/data/f.parquet"),
            }
        )
        loc = cfg.get_location("duckdb")
        assert isinstance(loc, FileStorageLocationConfig)
        assert loc.format == "parquet"

    def test_get_location_missing(self):
        cfg = CollectionPluginConfig()
        assert cfg.get_location("duckdb") is None


class TestConfigExamples:
    """Validate all documented config examples."""

    def test_pg_only_default(self):
        cfg = CollectionPluginConfig()
        assert cfg.write_driver_id == "postgresql"
        assert cfg.read_drivers == {}
        assert cfg.secondary_drivers == []

    def test_pg_plus_es_secondary(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            secondary_drivers=["elasticsearch"],
        )
        assert cfg.write_driver_id == "postgresql"
        assert cfg.secondary_driver_ids == ["elasticsearch"]

    def test_search_routed_to_es(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"search": "elasticsearch"},
            secondary_drivers=["elasticsearch"],
        )
        assert cfg.resolve_read_driver_id("search") == "elasticsearch"

    def test_dual_es_modes(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"search": "elasticsearch"},
            secondary_drivers=["elasticsearch", "elasticsearch_obfuscated"],
        )
        assert len(cfg.secondary_drivers) == 2
        assert "elasticsearch_obfuscated" in cfg.secondary_driver_ids

    def test_analytical_workload(self):
        cfg = CollectionPluginConfig(
            write_driver="iceberg",
            read_drivers={"analytics": "duckdb", "default": "duckdb"},
        )
        assert cfg.write_driver_id == "iceberg"
        assert cfg.resolve_read_driver_id("analytics") == "duckdb"
        assert cfg.resolve_read_driver_id("default") == "duckdb"

    def test_pg_with_storage_location(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            storage_locations={
                "postgresql": PostgresStorageLocationConfig(
                    physical_schema="custom_schema",
                    physical_table="custom_table",
                ),
            },
        )
        loc = cfg.get_location("postgresql")
        assert isinstance(loc, PostgresStorageLocationConfig)
        assert loc.physical_schema == "custom_schema"

    def test_duckdb_parquet(self):
        cfg = CollectionPluginConfig(
            write_driver="duckdb",
            storage_locations={
                "duckdb": FileStorageLocationConfig(
                    format="parquet",
                    path="/data/my_collection.parquet",
                ),
            },
        )
        loc = cfg.get_location("duckdb")
        assert isinstance(loc, FileStorageLocationConfig)
        assert loc.format == "parquet"


class TestSerializationRoundtrip:
    def test_model_dump_and_load(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"search": "elasticsearch"},
            secondary_drivers=["elasticsearch"],
        )
        data = cfg.model_dump()
        restored = CollectionPluginConfig.model_validate(data)
        assert restored.write_driver_id == cfg.write_driver_id
        assert restored.resolve_read_driver_id("search") == "elasticsearch"
        assert restored.secondary_driver_ids == cfg.secondary_driver_ids

    def test_json_roundtrip(self):
        cfg = CollectionPluginConfig(
            write_driver="postgresql",
            read_drivers={"search": "elasticsearch", "default": "postgresql"},
            secondary_drivers=["elasticsearch", "elasticsearch_obfuscated"],
        )
        json_str = cfg.model_dump_json()
        restored = CollectionPluginConfig.model_validate_json(json_str)
        assert restored.write_driver_id == cfg.write_driver_id
        assert restored.secondary_driver_ids == cfg.secondary_driver_ids
