import pytest


class TestDriverMeta:
    def test_class_name_matches_routing_contract(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        assert CollectionBigQueryDriver.__name__ == "CollectionBigQueryDriver"

    def test_capabilities_phase4a_set(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        d = CollectionBigQueryDriver()
        caps = d.capabilities
        assert "READ" in caps
        assert "STREAMING" in caps
        assert "INTROSPECTION" in caps
        assert "COUNT" in caps
        assert "AGGREGATION" in caps
        assert "WRITE" not in caps

    def test_preferred_for_features(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        d = CollectionBigQueryDriver()
        assert "features" in d.preferred_for

    def test_is_available_false_when_no_bq_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        monkeypatch.setattr(mod, "_get_bq_service", lambda: None)
        d = CollectionBigQueryDriver()
        assert d.is_available() is False


class TestReadEntities:
    @pytest.mark.asyncio
    async def test_read_entities_requires_fully_qualified_target(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p"),
            )),
        )
        with pytest.raises(ValueError):
            async for _ in d.read_entities("cat", "col"):
                pass

    @pytest.mark.asyncio
    async def test_read_entities_streams_via_bq_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake_service = type("S", (), {})()
        fake_service.execute_query = AsyncMock(side_effect=[
            [{"id": "a"}, {"id": "b"}],
            [],
        ])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                page_size=10,
            )),
        )

        feats = [f async for f in d.read_entities("cat", "col", limit=10)]
        assert [f.id for f in feats] == ["a", "b"]
        assert fake_service.execute_query.called


class TestCountAndAggregate:
    @pytest.mark.asyncio
    async def test_count_entities_runs_count_star(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 42}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        assert await d.count_entities("cat", "col") == 42
        called_sql = fake.execute_query.call_args[0][0]
        assert "COUNT(*)" in called_sql.upper()

    @pytest.mark.asyncio
    async def test_aggregate_sum(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 100.0}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        result = await d.aggregate("cat", "col", aggregation_type="sum", field="value")
        assert result == 100.0

    @pytest.mark.asyncio
    async def test_aggregate_rejects_unsafe_field(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 0}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        with pytest.raises(ValueError):
            await d.aggregate("cat", "col", aggregation_type="sum", field="x; DROP TABLE")


class TestIntrospect:
    @pytest.mark.asyncio
    async def test_introspect_schema_translates_bq_fields_to_field_definitions(
        self, monkeypatch,
    ):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, CollectionBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake_schema = []
        for name, ftype, mode in [
            ("id",    "STRING",    "REQUIRED"),
            ("value", "INTEGER",   "NULLABLE"),
            ("geom",  "GEOGRAPHY", "NULLABLE"),
        ]:
            f = MagicMock()
            f.name = name
            f.field_type = ftype
            f.mode = mode
            fake_schema.append(f)
        fake_table = MagicMock()
        fake_table.schema = fake_schema

        def fake_client_factory(project_id):
            c = MagicMock()
            c.get_table = MagicMock(return_value=fake_table)
            return c
        monkeypatch.setattr(mod, "_make_bq_client", fake_client_factory)

        d = CollectionBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=CollectionBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        fields = await d.introspect_schema("cat", "col")
        names = [f.name for f in fields]
        assert names == ["id", "value", "geom"]


def test_driver_discovered_via_entry_point():
    from importlib.metadata import entry_points
    eps = [ep for ep in entry_points(group="dynastore.modules") if ep.name == "storage_bigquery"]
    assert len(eps) == 1
    assert "CollectionBigQueryDriver" in eps[0].value
