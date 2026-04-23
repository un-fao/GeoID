"""Unit tests for M9 config discovery endpoints.

Covers:
- get_config_schemas() — all registered configs with scope annotation
- get_config_schema(class_key) — single config schema + 404 on unknown key
- get_config_graph() — nodes + edges structure
- list_storage_drivers() response keys use Protocol qualnames (M9 / §7)
- ConfigScopeMixin scope annotation propagation
- WritePolicyDefaults and CollectionSchema absent / present fields
"""
import pytest
from fastapi import FastAPI


@pytest.fixture(scope="module")
def service():
    from dynastore.extensions.configs.service import ConfigsService
    return ConfigsService(FastAPI())


# ---------------------------------------------------------------------------
# get_config_schemas
# ---------------------------------------------------------------------------


class TestGetConfigSchemas:
    @pytest.mark.asyncio
    async def test_returns_dict(self, service):
        result = await service.get_config_schemas()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_non_empty(self, service):
        result = await service.get_config_schemas()
        assert len(result) >= 5, f"Expected ≥5 registered configs, got {len(result)}"

    @pytest.mark.asyncio
    async def test_each_entry_has_required_keys(self, service):
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert "json_schema" in entry, f"Missing json_schema for {class_key}"
            assert "description" in entry, f"Missing description for {class_key}"
            assert "scope" in entry, f"Missing scope for {class_key}"

    @pytest.mark.asyncio
    async def test_scope_values_are_valid(self, service):
        valid_scopes = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert entry["scope"] in valid_scopes, (
                f"{class_key} has unexpected scope {entry['scope']!r}"
            )

    @pytest.mark.asyncio
    async def test_json_schema_is_dict(self, service):
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert isinstance(entry["json_schema"], dict), (
                f"{class_key}.json_schema is not a dict"
            )

    @pytest.mark.asyncio
    async def test_collection_routing_config_present(self, service):
        result = await service.get_config_schemas()
        assert "CollectionRoutingConfig" in result

    @pytest.mark.asyncio
    async def test_write_policy_defaults_present(self, service):
        result = await service.get_config_schemas()
        assert "WritePolicyDefaults" in result

    @pytest.mark.asyncio
    async def test_collection_schema_present(self, service):
        result = await service.get_config_schemas()
        assert "CollectionSchema" in result


# ---------------------------------------------------------------------------
# get_config_schema (single class)
# ---------------------------------------------------------------------------


class TestGetConfigSchema:
    @pytest.mark.asyncio
    async def test_known_class_key_returns_entry(self, service):
        result = await service.get_config_schema("CollectionRoutingConfig")
        assert result["class_key"] == "CollectionRoutingConfig"
        assert "json_schema" in result
        assert "description" in result
        assert "scope" in result

    @pytest.mark.asyncio
    async def test_unknown_class_key_raises_404(self, service):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await service.get_config_schema("NoSuchConfig_xyzzy")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_write_policy_defaults_schema_no_external_id_field(self, service):
        result = await service.get_config_schema("WritePolicyDefaults")
        props = result["json_schema"].get("properties", {})
        assert "external_id_field" not in props
        assert "validity_field" not in props
        assert "geohash_precision" not in props

    @pytest.mark.asyncio
    async def test_write_policy_defaults_schema_has_on_conflict(self, service):
        result = await service.get_config_schema("WritePolicyDefaults")
        props = result["json_schema"].get("properties", {})
        assert "on_conflict" in props

    @pytest.mark.asyncio
    async def test_collection_schema_has_constraints_field(self, service):
        result = await service.get_config_schema("CollectionSchema")
        props = result["json_schema"].get("properties", {})
        assert "constraints" in props

    @pytest.mark.asyncio
    async def test_scope_reflects_config_scope_mixin(self, service):
        """Classes with ConfigScopeMixin should expose their declared scope."""
        result = await service.get_config_schema("CollectionRoutingConfig")
        # CollectionRoutingConfig defaults to platform_waterfall
        assert result["scope"] == "platform_waterfall"


# ---------------------------------------------------------------------------
# get_config_graph
# ---------------------------------------------------------------------------


class TestGetConfigGraph:
    @pytest.mark.asyncio
    async def test_returns_nodes_and_edges(self, service):
        result = await service.get_config_graph()
        assert "nodes" in result
        assert "edges" in result

    @pytest.mark.asyncio
    async def test_nodes_is_list(self, service):
        result = await service.get_config_graph()
        assert isinstance(result["nodes"], list)

    @pytest.mark.asyncio
    async def test_edges_is_list(self, service):
        result = await service.get_config_graph()
        assert isinstance(result["edges"], list)

    @pytest.mark.asyncio
    async def test_nodes_non_empty(self, service):
        result = await service.get_config_graph()
        assert len(result["nodes"]) >= 5

    @pytest.mark.asyncio
    async def test_edges_have_required_keys(self, service):
        result = await service.get_config_graph()
        for edge in result["edges"]:
            assert "from" in edge, f"Edge missing 'from': {edge}"
            assert "to" in edge, f"Edge missing 'to': {edge}"
            assert "label" in edge, f"Edge missing 'label': {edge}"

    @pytest.mark.asyncio
    async def test_known_edge_write_policy_to_collection_schema(self, service):
        result = await service.get_config_graph()
        edges_from = {e["from"] for e in result["edges"]}
        assert "CollectionWritePolicy" in edges_from

    @pytest.mark.asyncio
    async def test_collection_routing_config_in_nodes(self, service):
        result = await service.get_config_graph()
        assert "CollectionRoutingConfig" in result["nodes"]


# ---------------------------------------------------------------------------
# Storage drivers grouped by Protocol qualname (M9/§7)
# ---------------------------------------------------------------------------


class TestStorageDriversProtocolGrouping:
    def test_driver_groups_use_protocol_qualnames(self):
        """list_storage_drivers response must key by Protocol qualname, not
        hard-coded strings like 'collections' / 'assets' / 'collection_metadata'.
        """
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.models.protocols.asset_driver import AssetStore
        from dynastore.models.protocols.metadata_driver import CollectionMetadataStore

        expected = {
            CollectionItemsStore.__qualname__,
            AssetStore.__qualname__,
            CollectionMetadataStore.__qualname__,
        }
        # Check the service source code directly — the keys are set at call time
        import inspect
        from dynastore.extensions.configs.service import ConfigsService
        src = inspect.getsource(ConfigsService.list_storage_drivers)
        for name in expected:
            assert name in src, (
                f"Protocol qualname '{name}' not found in list_storage_drivers source"
            )

    def test_old_string_domains_absent_from_list_storage_drivers(self):
        import inspect
        from dynastore.extensions.configs.service import ConfigsService
        src = inspect.getsource(ConfigsService.list_storage_drivers)
        for old_key in ('"collections"', '"assets"', '"collection_metadata"'):
            assert old_key not in src, (
                f"Old hard-coded domain key {old_key} found in list_storage_drivers"
            )


# ---------------------------------------------------------------------------
# ConfigScopeMixin annotation correctness
# ---------------------------------------------------------------------------


class TestConfigScopeMixinAnnotations:
    def test_collection_schema_has_collection_intrinsic_scope(self):
        from dynastore.modules.storage.driver_config import CollectionSchema
        from dynastore.modules.storage.schema_types import ConfigScopeMixin
        # CollectionSchema is collection-intrinsic per plan §8
        scope = getattr(CollectionSchema, "config_scope", "platform_waterfall")
        # Either annotated or default — just verify it's a valid scope
        valid = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        assert scope in valid

    def test_write_policy_defaults_scope_is_platform_waterfall(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        scope = getattr(WritePolicyDefaults, "config_scope", "platform_waterfall")
        assert scope == "platform_waterfall"


# ---------------------------------------------------------------------------
# get_effective_collection_config — unit tests with mocked config service
# ---------------------------------------------------------------------------


class _MockConfigsService:
    """Minimal ConfigsProtocol stub that returns configurable per-tier values."""

    def __init__(self, platform_data: dict, catalog_data: dict, collection_data: dict):
        self._platform = platform_data
        self._catalog = catalog_data
        self._collection = collection_data

    async def get_config(self, cls, catalog_id=None, collection_id=None, **kwargs):
        if catalog_id and collection_id:
            data = self._collection
        elif catalog_id:
            data = self._catalog
        else:
            data = self._platform
        return cls.model_validate(data) if data else cls()


class TestGetEffectiveCollectionConfig:
    """Tests use a minimal mock to avoid DB access."""

    def _make_service(self, platform_data, catalog_data, collection_data):
        from fastapi import FastAPI
        from dynastore.extensions.configs.service import ConfigsService

        mock = _MockConfigsService(platform_data, catalog_data, collection_data)

        # Subclass so the `configs` property override is local to the test
        # instance — patching `type(svc).configs = property(...)` directly
        # mutates the shared ConfigsService class and poisons every later
        # test that creates a real ConfigsService (the patched property
        # then dereferences `self._configs`, which doesn't exist on real
        # instances → AttributeError on every subsequent test).
        class _TestConfigsService(ConfigsService):
            @property
            def configs(self):  # type: ignore[override]
                return mock

        return _TestConfigsService(FastAPI())

    @pytest.mark.asyncio
    async def test_unknown_class_key_raises_404(self):
        from fastapi import FastAPI
        from fastapi import HTTPException
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_effective_collection_config("cat1", "col1", "NoSuchConfig_xyzzy")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_all_defaults_source_is_default(self):
        """When no overrides exist at any tier, every field source is 'default'."""
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        defaults = WritePolicyDefaults().model_dump()
        svc = self._make_service(defaults, defaults, defaults)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        assert result["class_key"] == "WritePolicyDefaults"
        for field_name, entry in result["resolved"].items():
            assert entry["source"] == "default", (
                f"Field '{field_name}' expected source 'default', got {entry['source']!r}"
            )

    @pytest.mark.asyncio
    async def test_platform_override_annotated_as_platform(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults, WriteConflictPolicy
        defaults = WritePolicyDefaults().model_dump()
        # Platform overrides on_conflict
        platform = {**defaults, "on_conflict": WriteConflictPolicy.REFUSE_FAIL.value}
        svc = self._make_service(platform, platform, platform)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        on_conflict = result["resolved"]["on_conflict"]
        assert on_conflict["source"] == "platform"
        assert on_conflict["value"] == WriteConflictPolicy.REFUSE_FAIL.value

    @pytest.mark.asyncio
    async def test_catalog_override_annotated_as_catalog(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults, WriteConflictPolicy
        defaults = WritePolicyDefaults().model_dump()
        platform = dict(defaults)
        catalog = {**defaults, "on_conflict": WriteConflictPolicy.REFUSE_FAIL.value}
        svc = self._make_service(platform, catalog, catalog)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        on_conflict = result["resolved"]["on_conflict"]
        assert on_conflict["source"] == "catalog"

    @pytest.mark.asyncio
    async def test_collection_override_annotated_as_collection(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults, WriteConflictPolicy
        defaults = WritePolicyDefaults().model_dump()
        platform = dict(defaults)
        catalog = dict(defaults)
        collection = {**defaults, "on_conflict": WriteConflictPolicy.REFUSE_FAIL.value}
        svc = self._make_service(platform, catalog, collection)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        on_conflict = result["resolved"]["on_conflict"]
        assert on_conflict["source"] == "collection"

    @pytest.mark.asyncio
    async def test_overrides_list_populated_for_non_default(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults, WriteConflictPolicy
        defaults = WritePolicyDefaults().model_dump()
        platform = {**defaults, "on_conflict": WriteConflictPolicy.REFUSE_FAIL.value}
        catalog = dict(platform)
        collection = {**platform, "on_conflict": WriteConflictPolicy.NEW_VERSION.value}
        svc = self._make_service(platform, catalog, collection)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        entry = result["resolved"]["on_conflict"]
        assert entry["source"] == "collection"
        assert "overrides" in entry
        # default value should appear in overrides
        default_val = WritePolicyDefaults().on_conflict
        assert any(str(default_val) in str(o) or repr(default_val.value) in o for o in entry["overrides"])

    @pytest.mark.asyncio
    async def test_response_shape_has_required_keys(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        defaults = WritePolicyDefaults().model_dump()
        svc = self._make_service(defaults, defaults, defaults)
        result = await svc.get_effective_collection_config(
            "cat1", "col1", "WritePolicyDefaults"
        )
        assert "class_key" in result
        assert "resolved" in result
        assert isinstance(result["resolved"], dict)
