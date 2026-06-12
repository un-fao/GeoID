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

"""Unit tests for BigQuery ensure_storage + _build_bq_schema_from_projection (#1295)."""

import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# google.cloud stub — google-cloud-bigquery is not installed in the test venv;
# inject a minimal stub so functions that do ``from google.cloud import bigquery``
# can be called without the real package.  We create real-ish SchemaField objects
# so the schema-builder tests are not trivially vacuous.
# ---------------------------------------------------------------------------


class _FakeSchemaField:
    """Minimal bigquery.SchemaField surrogate."""
    def __init__(self, name: str, field_type: str, mode: str = "NULLABLE"):
        self.name = name
        self.field_type = field_type
        self.mode = mode

    def __eq__(self, other):
        return (
            isinstance(other, _FakeSchemaField)
            and self.name == other.name
            and self.field_type == other.field_type
            and self.mode == other.mode
        )

    def __repr__(self):
        return f"SchemaField({self.name!r}, {self.field_type!r}, mode={self.mode!r})"


class _FakeDataset:
    def __init__(self, ref: str):
        self._ref = ref
        self.location = None


class _FakeTable:
    def __init__(self, fqn: str, schema=None):
        self._fqn = fqn
        self.schema = schema or []


def _make_fake_bigquery_module() -> types.ModuleType:
    bq = types.ModuleType("google.cloud.bigquery")
    bq.SchemaField = _FakeSchemaField
    bq.Dataset = _FakeDataset
    bq.Table = _FakeTable
    return bq


def _make_fake_exceptions_module() -> types.ModuleType:
    exc = types.ModuleType("google.cloud.exceptions")

    class NotFound(Exception):
        pass

    exc.NotFound = NotFound
    return exc


def _inject_google_stubs():
    """Insert google.cloud.bigquery + google.cloud.exceptions into sys.modules."""
    if "google" not in sys.modules:
        google = types.ModuleType("google")
        sys.modules["google"] = google
    else:
        google = sys.modules["google"]

    if "google.cloud" not in sys.modules:
        cloud = types.ModuleType("google.cloud")
        sys.modules["google.cloud"] = cloud
        setattr(google, "cloud", cloud)
    else:
        cloud = sys.modules["google.cloud"]

    bq = _make_fake_bigquery_module()
    sys.modules["google.cloud.bigquery"] = bq
    setattr(cloud, "bigquery", bq)

    exc = _make_fake_exceptions_module()
    sys.modules["google.cloud.exceptions"] = exc
    setattr(cloud, "exceptions", exc)

    return bq, exc


# Inject at module import time so all tests in this file can import the driver.
_FAKE_BQ, _FAKE_EXCEPTIONS = _inject_google_stubs()
_NotFound = _FAKE_EXCEPTIONS.NotFound


# ---------------------------------------------------------------------------
# _build_bq_schema_from_projection — pure function, uses real FieldDefinition
# ---------------------------------------------------------------------------


class TestBuildBqSchemaFromProjection:
    """Every canonical type must map to the correct BQ SchemaField type."""

    def _fd(self, name: str, data_type: str, required: bool = False, capabilities=None):
        from dynastore.models.protocols.field_definition import FieldDefinition
        caps = capabilities or []
        return FieldDefinition(name=name, data_type=data_type, required=required, capabilities=caps)

    def test_all_canonical_types(self):
        """Each canonical type maps to the documented BQ type."""
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        cases = [
            ("f_string",    "string",    "STRING"),
            ("f_bigint",    "bigint",    "INT64"),
            ("f_double",    "double",    "FLOAT64"),
            ("f_numeric",   "numeric",   "NUMERIC"),
            ("f_boolean",   "boolean",   "BOOL"),
            ("f_timestamp", "timestamp", "TIMESTAMP"),
            ("f_date",      "date",      "DATE"),
            ("f_time",      "time",      "TIME"),
            ("f_binary",    "binary",    "BYTES"),
            ("f_jsonb",     "jsonb",     "JSON"),
            ("f_geometry",  "geometry",  "GEOGRAPHY"),
        ]
        projected = {name: self._fd(name, dtype) for name, dtype, _ in cases}
        result = _build_bq_schema_from_projection(projected)
        by_name = {f.name: f for f in result}

        for name, _, expected_bq_type in cases:
            assert by_name[name].field_type == expected_bq_type, (
                f"{name}: expected {expected_bq_type}, got {by_name[name].field_type}"
            )

    def test_required_field_mode_is_required(self):
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        projected = {"id": self._fd("id", "string", required=True)}
        result = _build_bq_schema_from_projection(projected)
        assert result[0].mode == "REQUIRED"

    def test_nullable_field_mode_is_nullable(self):
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        projected = {"value": self._fd("value", "double", required=False)}
        result = _build_bq_schema_from_projection(projected)
        assert result[0].mode == "NULLABLE"

    def test_fulltext_capability_yields_string_column(self):
        """FULLTEXT fields are STRING on BQ — no analyzed-vs-keyword distinction."""
        from dynastore.models.protocols.field_definition import FieldCapability
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        projected = {
            "description": self._fd(
                "description", "string",
                capabilities=[FieldCapability.FULLTEXT],
            ),
        }
        result = _build_bq_schema_from_projection(projected)
        assert result[0].field_type == "STRING"

    def test_filterable_capability_yields_string_column(self):
        from dynastore.models.protocols.field_definition import FieldCapability
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        projected = {
            "code": self._fd(
                "code", "string",
                capabilities=[FieldCapability.FILTERABLE],
            ),
        }
        result = _build_bq_schema_from_projection(projected)
        assert result[0].field_type == "STRING"

    def test_unknown_canonical_type_falls_back_to_string(self):
        """uuid canonical type (not in BQ map) falls back to STRING."""
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        fd = self._fd("x", "string")
        # Temporarily swap data_type to a canonical token not in _CANONICAL_TO_BQ
        object.__setattr__(fd, "data_type", "uuid")
        projected = {"x": fd}
        result = _build_bq_schema_from_projection(projected)
        assert result[0].field_type == "STRING"

    def test_preserves_field_order(self):
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        projected = {
            "a": self._fd("a", "string"),
            "b": self._fd("b", "bigint"),
            "c": self._fd("c", "boolean"),
        }
        result = _build_bq_schema_from_projection(projected)
        assert [f.name for f in result] == ["a", "b", "c"]

    def test_empty_projection_yields_empty_schema(self):
        from dynastore.modules.storage.drivers.bigquery import _build_bq_schema_from_projection

        result = _build_bq_schema_from_projection({})
        assert result == []


# ---------------------------------------------------------------------------
# ensure_storage no-op gates
# ---------------------------------------------------------------------------


class TestEnsureStorageNoOp:
    """ensure_storage must never construct a BQ client when it should be a no-op."""

    @pytest.mark.asyncio
    async def test_noop_when_collection_id_is_none(self, monkeypatch):
        """Catalog-level call (collection_id=None) is always a no-op."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        monkeypatch.setattr(
            mod, "_make_bq_client",
            lambda *a, **kw: (_ for _ in ()).throw(
                AssertionError("_make_bq_client must not be called")
            ),
        )
        d = ItemsBigQueryDriver()
        await d.ensure_storage("cat", collection_id=None)

    @pytest.mark.asyncio
    async def test_noop_when_manage_storage_false(self, monkeypatch):
        """Default manage_storage=False must NOT construct a BQ client."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )

        client_built = {"called": False}

        def fail_if_called(*a, **kw):
            client_built["called"] = True
            raise AssertionError("_make_bq_client must not be called when manage_storage=False")

        monkeypatch.setattr(mod, "_make_bq_client", fail_if_called)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                # manage_storage defaults to False
            )),
        )
        await d.ensure_storage("cat", "col")
        assert not client_built["called"]

    @pytest.mark.asyncio
    async def test_noop_when_config_is_none(self, monkeypatch):
        """No config returned -> no-op (driver not configured for this collection)."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        client_built = {"called": False}

        def fail_if_called(*a, **kw):
            client_built["called"] = True
            raise AssertionError("must not build client when cfg is None")

        monkeypatch.setattr(mod, "_make_bq_client", fail_if_called)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=None))
        await d.ensure_storage("cat", "col")
        assert not client_built["called"]

    @pytest.mark.asyncio
    async def test_raises_when_target_not_fully_qualified(self, monkeypatch):
        """manage_storage=True but incomplete target -> ValueError."""
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p"),  # incomplete
                manage_storage=True,
            )),
        )
        with pytest.raises(ValueError, match="fully-qualified"):
            await d.ensure_storage("cat", "col")


# ---------------------------------------------------------------------------
# ensure_storage provisioning logic (manage_storage=True)
# ---------------------------------------------------------------------------


class TestEnsureStorageProvisioning:
    """Provisioning path — BQ client is constructed and called appropriately."""

    def _full_cfg(self, manage_storage: bool = True):
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        return ItemsBigQueryDriverConfig(
            target=BigQueryTarget(project_id="my-proj", dataset_id="my_ds", table_name="items"),
            manage_storage=manage_storage,
            location="US",
        )

    def _make_fake_client(self, *, table_exists: bool):
        """Build a fake synchronous BQ client using our stubs."""
        fake = MagicMock()
        fake.create_dataset = MagicMock()
        fake.create_table = MagicMock()
        fake.close = MagicMock()
        if table_exists:
            fake.get_table = MagicMock(return_value=MagicMock())
        else:
            fake.get_table = MagicMock(side_effect=_NotFound("table"))
        return fake

    @pytest.mark.asyncio
    async def test_creates_dataset_and_table_when_absent(self, monkeypatch):
        """New table: create_dataset + create_table are called once."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        fake_client = self._make_fake_client(table_exists=False)
        monkeypatch.setattr(mod, "_make_bq_client", lambda *a, **kw: fake_client)
        monkeypatch.setattr(mod, "_resolve_items_schema", AsyncMock(return_value=None))
        monkeypatch.setattr(mod, "_resolve_write_policy", AsyncMock(return_value=None))
        monkeypatch.setattr(mod, "_build_bq_schema_from_projection", lambda projected: [])

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=self._full_cfg()))

        await d.ensure_storage("cat", "col")

        fake_client.create_dataset.assert_called_once()
        fake_client.create_table.assert_called_once()
        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_create_table_when_already_exists(self, monkeypatch):
        """Existing table: create_table is NOT called (create-only-if-absent)."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        fake_client = self._make_fake_client(table_exists=True)
        monkeypatch.setattr(mod, "_make_bq_client", lambda *a, **kw: fake_client)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=self._full_cfg()))

        await d.ensure_storage("cat", "col")

        fake_client.create_dataset.assert_called_once()
        fake_client.create_table.assert_not_called()
        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_dataset_location_is_set_from_config(self, monkeypatch):
        """Dataset object must carry cfg.location before create_dataset is called."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        fake_client = self._make_fake_client(table_exists=False)
        monkeypatch.setattr(mod, "_make_bq_client", lambda *a, **kw: fake_client)
        monkeypatch.setattr(mod, "_resolve_items_schema", AsyncMock(return_value=None))
        monkeypatch.setattr(mod, "_resolve_write_policy", AsyncMock(return_value=None))
        monkeypatch.setattr(mod, "_build_bq_schema_from_projection", lambda p: [])

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=self._full_cfg()))

        await d.ensure_storage("cat", "col")

        call_args = fake_client.create_dataset.call_args
        dataset_arg = call_args[0][0]
        assert dataset_arg.location == "US"

    @pytest.mark.asyncio
    async def test_schema_built_from_materialize_feature_fields(self, monkeypatch):
        """Table schema is derived from materialize_feature_fields, not ad-hoc."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.models.protocols.field_definition import FieldDefinition
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        fake_client = self._make_fake_client(table_exists=False)
        monkeypatch.setattr(mod, "_make_bq_client", lambda *a, **kw: fake_client)

        sentinel_field = _FakeSchemaField("sentinel_col", "STRING")

        # Patch materialize_feature_fields inside field_projection module
        fake_fd = FieldDefinition(name="my_field", data_type="string")
        fake_projected = {"my_field": fake_fd}

        monkeypatch.setattr(mod, "_resolve_items_schema", AsyncMock(return_value=None))
        monkeypatch.setattr(mod, "_resolve_write_policy", AsyncMock(return_value=None))

        # Patch _build_bq_schema_from_projection to capture the projected dict
        captured_projected = {}

        def fake_build(projected):
            captured_projected.update(projected)
            return [sentinel_field]

        monkeypatch.setattr(mod, "_build_bq_schema_from_projection", fake_build)

        # Patch materialize_feature_fields to return our known projection
        import dynastore.modules.storage.field_projection as fp_mod
        monkeypatch.setattr(
            fp_mod, "materialize_feature_fields",
            lambda schema, policy: fake_projected,
        )

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=self._full_cfg()))
        await d.ensure_storage("cat", "col")

        # The projected map came from materialize_feature_fields
        assert "my_field" in captured_projected
        # The table passed to create_table carries our sentinel schema
        create_table_call = fake_client.create_table.call_args[0][0]
        assert create_table_call.schema == [sentinel_field]

    @pytest.mark.asyncio
    async def test_client_closed_on_error(self, monkeypatch):
        """BQ client is always closed — even when create_dataset raises."""
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        fake_client = MagicMock()
        fake_client.create_dataset = MagicMock(side_effect=RuntimeError("quota"))
        fake_client.close = MagicMock()
        monkeypatch.setattr(mod, "_make_bq_client", lambda *a, **kw: fake_client)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=self._full_cfg()))

        with pytest.raises(RuntimeError, match="quota"):
            await d.ensure_storage("cat", "col")

        fake_client.close.assert_called_once()


# ---------------------------------------------------------------------------
# manage_storage config field — model-level contract
# ---------------------------------------------------------------------------


class TestManageStorageConfig:
    def test_default_is_false(self):
        from dynastore.modules.storage.drivers.bigquery_models import ItemsBigQueryDriverConfig
        cfg = ItemsBigQueryDriverConfig()
        assert cfg.manage_storage is False

    def test_can_be_set_to_true(self):
        from dynastore.modules.storage.drivers.bigquery_models import ItemsBigQueryDriverConfig
        cfg = ItemsBigQueryDriverConfig(manage_storage=True)
        assert cfg.manage_storage is True

    def test_empty_model_dump_excludes_manage_storage_when_unset(self):
        """A bare config must still produce an empty model_dump(exclude_unset=True)."""
        from dynastore.modules.storage.drivers.bigquery_models import ItemsBigQueryDriverConfig
        cfg = ItemsBigQueryDriverConfig()
        dumped = cfg.model_dump(exclude_unset=True)
        assert "manage_storage" not in dumped
