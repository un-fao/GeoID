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

"""Unit tests for MaintenancePendingOwner.

Covers:
- describe_scope CATALOG: resolves physical_schema and returns one CleanupRef
  with schema + catalog_id in metadata and no collection_id.
- describe_scope COLLECTION: CleanupRef metadata carries collection_id.
- describe_scope: unresolvable physical_schema returns empty list.
- cleanup_one SOFT: no-op, returns DONE without opening a DB connection.
- cleanup_one HARD dry_run: logs and returns DONE without touching the DB.
- cleanup_one HARD: issues UPDATE on tasks and events, returns DONE.
- cleanup_one HARD COLLECTION scope: UPDATE predicates include collection_id.
- cleanup_one HARD: UPDATE SQL excludes schema_name='system'.
- cleanup_one HARD: missing schema in metadata returns DEAD.
- cleanup_one HARD: no DB engine returns RETRY.
- cleanup_one HARD: exception from DB returns RETRY.
- register_owners: adds owner to registry under CATALOG and COLLECTION scopes.

All tests are DB-free: async database calls are replaced with AsyncMock.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry
from dynastore.modules.catalog.resource_owner import (
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

_CATALOG_ID = "cat-abc"
_COLLECTION_ID = "coll-xyz"
_PHYSICAL_SCHEMA = "s_catABC"


def _make_owner():
    from dynastore.modules.catalog.maintenance_cascade_owner import MaintenancePendingOwner
    return MaintenancePendingOwner()


def _make_catalog_ref(*, collection_id: str | None = None) -> CleanupRef:
    meta: dict[str, Any] = {"schema": _PHYSICAL_SCHEMA, "catalog_id": _CATALOG_ID}
    if collection_id is not None:
        meta["collection_id"] = collection_id
    locator = _CATALOG_ID if collection_id is None else f"{_CATALOG_ID}/{collection_id}"
    return CleanupRef(
        kind="maintenance_pending",
        locator=locator,
        owner_id="maintenance.pending_work",
        metadata=meta,
    )


# ---------------------------------------------------------------------------
# describe_scope
# ---------------------------------------------------------------------------


class TestDescribeScope:
    @pytest.mark.asyncio
    async def test_catalog_scope_resolves_schema_and_returns_ref(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        mock_dql = MagicMock()
        mock_dql.execute = AsyncMock(return_value=_PHYSICAL_SCHEMA)

        with patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            return_value=mock_dql,
        ):
            refs = await owner.describe_scope(scope_ref, AsyncMock())

        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "maintenance_pending"
        assert ref.owner_id == "maintenance.pending_work"
        assert ref.metadata["schema"] == _PHYSICAL_SCHEMA
        assert ref.metadata["catalog_id"] == _CATALOG_ID
        assert "collection_id" not in ref.metadata
        assert ref.locator == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_collection_scope_carries_collection_id_in_metadata(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )

        mock_dql = MagicMock()
        mock_dql.execute = AsyncMock(return_value=_PHYSICAL_SCHEMA)

        with patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            return_value=mock_dql,
        ):
            refs = await owner.describe_scope(scope_ref, AsyncMock())

        assert len(refs) == 1
        ref = refs[0]
        assert ref.metadata["collection_id"] == _COLLECTION_ID
        assert ref.locator == f"{_CATALOG_ID}/{_COLLECTION_ID}"

    @pytest.mark.asyncio
    async def test_unresolvable_physical_schema_returns_empty(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="ghost")

        mock_dql = MagicMock()
        mock_dql.execute = AsyncMock(return_value=None)

        with patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            return_value=mock_dql,
        ):
            refs = await owner.describe_scope(scope_ref, AsyncMock())

        assert refs == []


# ---------------------------------------------------------------------------
# cleanup_one — SOFT and dry_run
# ---------------------------------------------------------------------------


class TestCleanupOneSoftAndDryRun:
    @pytest.mark.asyncio
    async def test_soft_mode_returns_done_without_db(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()
        # get_engine must not be called in SOFT mode
        with patch(
            "dynastore.tools.protocol_helpers.get_engine",
            side_effect=AssertionError("get_engine must not be called in SOFT mode"),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_dry_run_hard_returns_done_without_db(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()
        with patch(
            "dynastore.tools.protocol_helpers.get_engine",
            side_effect=AssertionError("get_engine must not be called in dry-run"),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD, dry_run=True)
        assert outcome == CleanupOutcome.DONE


# ---------------------------------------------------------------------------
# cleanup_one — HARD, normal path
# ---------------------------------------------------------------------------


def _make_fake_txn(mock_conn: Any):
    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield mock_conn

    return _fake_managed_transaction


class TestCleanupOneHard:
    @pytest.mark.asyncio
    async def test_hard_catalog_scope_returns_done(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()

        mock_conn = AsyncMock()
        mock_dql_inst = MagicMock()
        mock_dql_inst.execute = AsyncMock(side_effect=[3, 2])
        mock_engine = MagicMock()

        with (
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
                new=_make_fake_txn(mock_conn),
            ),
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                return_value=mock_dql_inst,
            ),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE
        # DQLQuery.execute called twice — once for tasks, once for events
        assert mock_dql_inst.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_hard_collection_scope_sql_includes_collection_id(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref(collection_id=_COLLECTION_ID)

        mock_conn = AsyncMock()
        mock_engine = MagicMock()
        captured_sqls: list[str] = []

        mock_dql_inst = MagicMock()
        mock_dql_inst.execute = AsyncMock(side_effect=[1, 0])

        def _capture_dql(sql: str, **kwargs: Any):
            captured_sqls.append(sql)
            return mock_dql_inst

        with (
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
                new=_make_fake_txn(mock_conn),
            ),
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                side_effect=_capture_dql,
            ),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE
        assert len(captured_sqls) == 2
        for sql in captured_sqls:
            assert "collection_id" in sql, (
                f"Expected collection_id predicate in SQL but got:\n{sql}"
            )

    @pytest.mark.asyncio
    async def test_hard_sql_excludes_system_schema_name(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()

        mock_conn = AsyncMock()
        mock_engine = MagicMock()
        captured_sqls: list[str] = []

        mock_dql_inst = MagicMock()
        mock_dql_inst.execute = AsyncMock(side_effect=[0, 0])

        def _capture_dql(sql: str, **kwargs: Any):
            captured_sqls.append(sql)
            return mock_dql_inst

        with (
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
                new=_make_fake_txn(mock_conn),
            ),
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                side_effect=_capture_dql,
            ),
        ):
            await owner.cleanup_one(ref, CleanupMode.HARD)

        assert len(captured_sqls) == 2
        for sql in captured_sqls:
            assert "system" in sql, (
                f"Expected 'system' exclusion guard in SQL but got:\n{sql}"
            )

    @pytest.mark.asyncio
    async def test_hard_missing_schema_in_metadata_returns_dead(self) -> None:
        owner = _make_owner()
        ref = CleanupRef(
            kind="maintenance_pending",
            locator=_CATALOG_ID,
            owner_id="maintenance.pending_work",
            metadata={"catalog_id": _CATALOG_ID},  # no "schema" key
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.DEAD

    @pytest.mark.asyncio
    async def test_hard_no_engine_returns_retry(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()
        with patch(
            "dynastore.tools.protocol_helpers.get_engine",
            return_value=None,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_hard_db_exception_returns_retry(self) -> None:
        owner = _make_owner()
        ref = _make_catalog_ref()
        mock_engine = MagicMock()

        @asynccontextmanager
        async def _exploding_txn(_engine):
            raise RuntimeError("DB connection refused")
            yield  # makes this an async generator (unreachable branch)

        with (
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
                new=_exploding_txn,
            ),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.RETRY


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_register_owners_adds_to_registry(self) -> None:
        from dynastore.modules.catalog.maintenance_cascade_owner import (
            MaintenancePendingOwner,
            register_owners,
        )

        reg = CascadeCleanupRegistry()
        register_owners(reg)

        assert reg.get(MaintenancePendingOwner.owner_id) is not None

    def test_owner_registered_under_catalog_scope(self) -> None:
        from dynastore.modules.catalog.maintenance_cascade_owner import (
            MaintenancePendingOwner,
            register_owners,
        )

        reg = CascadeCleanupRegistry()
        register_owners(reg)

        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        assert MaintenancePendingOwner.owner_id in catalog_owners

    def test_owner_registered_under_collection_scope(self) -> None:
        from dynastore.modules.catalog.maintenance_cascade_owner import (
            MaintenancePendingOwner,
            register_owners,
        )

        reg = CascadeCleanupRegistry()
        register_owners(reg)

        collection_owners = [
            o.owner_id for o in reg.owners_for_scope(ResourceScope.COLLECTION)
        ]
        assert MaintenancePendingOwner.owner_id in collection_owners

    def test_owner_id_is_stable(self) -> None:
        from dynastore.modules.catalog.maintenance_cascade_owner import MaintenancePendingOwner
        assert MaintenancePendingOwner.owner_id == "maintenance.pending_work"
