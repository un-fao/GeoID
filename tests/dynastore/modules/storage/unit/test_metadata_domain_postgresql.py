"""Unit tests for the M2.1 domain-scoped Primary metadata drivers.

Focuses on pure-python invariants that don't require a live database:

- Declared capabilities / domain / implemented protocol.
- ``_filter_payload`` correctly strips keys outside the driver's domain.
- Domain column sets match the M2.0 DDL.
- SQL-builder smoke tests with a mocked engine confirm the driver uses
  the right table, no column leakage across domains, and correctly
  stamps ``updated_at = NOW()`` on upsert.
- Entry-points expose the four new drivers under canonical names.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.driver_roles import MetadataDomain
from dynastore.models.protocols.metadata_driver import (
    CatalogMetadataStore,
    CollectionMetadataStore,
    MetadataCapability,
)


# ---------------------------------------------------------------------------
# Static invariants
# ---------------------------------------------------------------------------


class TestStructuralInvariants:
    def test_collection_core_driver_identifies_as_core_collection(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            CollectionCorePostgresqlDriver,
        )

        d = CollectionCorePostgresqlDriver()
        assert d.domain is MetadataDomain.CORE
        assert d._table == "collection_metadata_core"
        assert MetadataCapability.READ in d.capabilities
        assert MetadataCapability.WRITE in d.capabilities
        assert MetadataCapability.SEARCH in d.capabilities
        assert MetadataCapability.SOFT_DELETE in d.capabilities
        # Structural typing — CollectionMetadataStore is @runtime_checkable.
        assert isinstance(d, CollectionMetadataStore)

    def test_collection_stac_driver_identifies_as_stac_collection(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            CollectionStacPostgresqlDriver,
        )

        d = CollectionStacPostgresqlDriver()
        assert d.domain is MetadataDomain.STAC
        assert d._table == "collection_metadata_stac"
        assert MetadataCapability.SPATIAL_FILTER in d.capabilities
        # STAC collection driver does NOT advertise SEARCH — the CORE
        # driver owns title/description; STAC carries structured fields.
        assert MetadataCapability.SEARCH not in d.capabilities
        assert isinstance(d, CollectionMetadataStore)

    def test_catalog_core_driver_identifies_as_core_catalog(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            CatalogCorePostgresqlDriver,
        )

        d = CatalogCorePostgresqlDriver()
        assert d.domain is MetadataDomain.CORE
        assert d._table == "catalog_metadata_core"
        assert isinstance(d, CatalogMetadataStore)

    def test_catalog_stac_driver_identifies_as_stac_catalog(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            CatalogStacPostgresqlDriver,
        )

        d = CatalogStacPostgresqlDriver()
        assert d.domain is MetadataDomain.STAC
        assert d._table == "catalog_metadata_stac"
        assert isinstance(d, CatalogMetadataStore)


class TestDomainColumnSets:
    """Column sets must match the M2.0 DDL exactly.

    Guards against drift between the driver's ``_columns`` tuple and the
    CREATE TABLE statements in ``modules/catalog/db_init/metadata_domain_split.py``.
    A stray column on the Python side silently succeeds on UPDATE (the
    EXCLUDED alias accepts any column) but fails on INSERT with
    UndefinedColumn.  Pinning the column sets here means the test fails
    loudly at unit-test time.
    """

    def test_collection_core_columns(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _COLLECTION_CORE_COLUMNS,
        )

        assert _COLLECTION_CORE_COLUMNS == (
            "title", "description", "keywords", "license", "extra_metadata",
        )

    def test_collection_stac_columns(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _COLLECTION_STAC_COLUMNS,
        )

        assert _COLLECTION_STAC_COLUMNS == (
            "stac_version", "stac_extensions", "extent", "providers",
            "summaries", "links", "assets", "item_assets",
        )

    def test_catalog_core_columns(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _CATALOG_CORE_COLUMNS,
        )

        assert _CATALOG_CORE_COLUMNS == (
            "title", "description", "keywords", "license", "extra_metadata",
        )

    def test_catalog_stac_columns(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _CATALOG_STAC_COLUMNS,
        )

        assert _CATALOG_STAC_COLUMNS == (
            "stac_version", "stac_extensions", "conforms_to", "links", "assets",
        )


class TestPayloadFilter:
    def test_filter_payload_drops_unowned_keys(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _COLLECTION_CORE_COLUMNS, _filter_payload,
        )

        full = {
            # CORE
            "title": {"en": "T"}, "description": {"en": "D"},
            "keywords": ["k"], "license": "CC-BY-4.0",
            "extra_metadata": {"_internal": 1},
            # STAC (should be dropped by the CORE filter)
            "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}},
            "stac_version": "1.1.0",
            "providers": [{"name": "FAO"}],
        }
        filtered = _filter_payload(full, _COLLECTION_CORE_COLUMNS)
        assert set(filtered) == set(_COLLECTION_CORE_COLUMNS)
        assert "extent" not in filtered
        assert "stac_version" not in filtered
        assert filtered["title"] == {"en": "T"}

    def test_filter_payload_missing_keys_become_none(self):
        from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
            _COLLECTION_STAC_COLUMNS, _filter_payload,
        )

        filtered = _filter_payload({}, _COLLECTION_STAC_COLUMNS)
        assert set(filtered) == set(_COLLECTION_STAC_COLUMNS)
        for v in filtered.values():
            assert v is None


# ---------------------------------------------------------------------------
# Isolated CRUD behaviour (mocked engine)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_conn_with_dql():
    """Return (fake_engine, AsyncMock dql_execute) and patch DQLQuery.

    Lets each test inspect the exact SQL string and bind params the
    driver issued without needing a live DB.
    """
    import dynastore.modules.storage.drivers.metadata_domain_postgresql as mod

    fake_engine = MagicMock()
    dql_execute = AsyncMock(return_value=None)

    class _FakeDQL:
        def __init__(self, sql, result_handler=None):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            return await dql_execute(self.sql, **kwargs)

    fake_txn = AsyncMock()
    fake_txn.__aenter__ = AsyncMock(return_value=MagicMock())
    fake_txn.__aexit__ = AsyncMock(return_value=None)

    patches = [
        patch.object(mod, "DQLQuery", _FakeDQL),
        patch.object(mod, "managed_transaction", lambda engine: fake_txn),
        patch.object(mod, "_get_engine", return_value=fake_engine),
        patch.object(mod, "_resolve_physical_schema", AsyncMock(return_value="t_alpha")),
    ]
    for p in patches:
        p.start()
    try:
        yield fake_engine, dql_execute
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_collection_core_upsert_uses_core_columns_only(fake_conn_with_dql):
    from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
        CollectionCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionCorePostgresqlDriver()
    await d.upsert_metadata(
        "cat", "col",
        metadata={
            "title": {"en": "T"},
            "description": {"en": "D"},
            "extent": "SHOULD BE IGNORED — STAC COLUMN",
            "providers": ["IGNORE"],
        },
    )

    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert '"t_alpha".collection_metadata_core' in sql
    assert "extent" not in sql
    assert "providers" not in sql
    # CORE columns appear in both the column list and the ON CONFLICT
    # UPDATE set — so "title" shows up twice.  Assert both directions.
    assert sql.count("title") >= 2
    assert "NOW()" in sql  # updated_at stamping
    # Bind params cover all CORE columns plus :id — STAC leak would show here.
    assert set(params) == {"id", "title", "description", "keywords", "license", "extra_metadata"}


@pytest.mark.asyncio
async def test_collection_stac_upsert_uses_stac_columns_only(fake_conn_with_dql):
    from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
        CollectionStacPostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionStacPostgresqlDriver()
    await d.upsert_metadata(
        "cat", "col",
        metadata={
            "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}},
            "title": "SHOULD BE IGNORED — CORE COLUMN",
            "license": "IGNORE",
        },
    )

    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert '"t_alpha".collection_metadata_stac' in sql
    assert "title" not in sql
    assert "license" not in sql
    assert "extent" in sql
    assert "stac_version" in sql
    # Params: :id + 8 STAC columns.
    assert set(params) == {
        "id", "stac_version", "stac_extensions", "extent", "providers",
        "summaries", "links", "assets", "item_assets",
    }


@pytest.mark.asyncio
async def test_catalog_core_upsert_targets_global_schema(fake_conn_with_dql):
    from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
        CatalogCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CatalogCorePostgresqlDriver()
    await d.upsert_catalog_metadata(
        "cat",
        metadata={"title": {"en": "T"}, "description": {"en": "D"}},
    )

    dql_execute.assert_awaited_once()
    sql = dql_execute.call_args.args[0]
    # catalog-tier driver uses the global catalog schema, NOT {schema}.
    assert "catalog.catalog_metadata_core" in sql
    assert '"t_alpha"' not in sql  # no tenant schema reference
    assert "NOW()" in sql


@pytest.mark.asyncio
async def test_catalog_stac_upsert_carries_catalog_stac_columns(fake_conn_with_dql):
    from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
        CatalogStacPostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CatalogStacPostgresqlDriver()
    await d.upsert_catalog_metadata(
        "cat",
        metadata={
            "stac_version": "1.1.0",
            "conforms_to": ["http://…/core"],
            "item_assets": "SHOULD BE IGNORED — catalog STAC has no item_assets",
        },
    )
    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert "catalog.catalog_metadata_stac" in sql
    # Catalog STAC has conforms_to but NOT item_assets — sanity-check the leak.
    assert "conforms_to" in sql
    assert "item_assets" not in sql
    assert set(params) == {
        "id", "stac_version", "stac_extensions", "conforms_to", "links", "assets",
    }


# ---------------------------------------------------------------------------
# Entry-point discovery
# ---------------------------------------------------------------------------


class TestEntryPoints:
    def test_four_new_entry_points_registered(self):
        from importlib.metadata import entry_points

        expected = {
            "metadata_collection_core_postgresql": "CollectionCorePostgresqlDriver",
            "metadata_collection_stac_postgresql": "CollectionStacPostgresqlDriver",
            "metadata_catalog_core_postgresql": "CatalogCorePostgresqlDriver",
            "metadata_catalog_stac_postgresql": "CatalogStacPostgresqlDriver",
        }

        got = {
            ep.name: ep.value
            for ep in entry_points(group="dynastore.modules")
            if ep.name in expected
        }
        assert set(got) == set(expected), (
            "Missing entry points — run `pip install -e .` to refresh "
            "the installed metadata: "
            f"expected {set(expected) - set(got)}"
        )
        for name, expected_cls in expected.items():
            assert expected_cls in got[name], (
                f"Entry point {name} points at {got[name]!r}, "
                f"expected ...:{expected_cls}"
            )
