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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for the GET /admin/catalogs/{catalog_id} provisioning-view endpoint.

Exercises the handler logic in isolation: CatalogsProtocol, DatabaseProtocol,
tasks_module.list_tasks, and managed_transaction are replaced with stubs so no
database or service registry is required.
"""
from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.models.tasks import TaskStatusEnum


# ---------------------------------------------------------------------------
# Helpers — build fake catalog and task objects
# ---------------------------------------------------------------------------

def _make_catalog(catalog_id: str, provisioning_status: str = "ready") -> MagicMock:
    catalog = MagicMock()
    catalog.id = catalog_id
    catalog.provisioning_status = provisioning_status
    return catalog


def _make_task(
    task_type: str = "gcp_provision_catalog",
    status: TaskStatusEnum = TaskStatusEnum.COMPLETED,
    error_message: str | None = None,
    retry_count: int = 0,
    max_retries: int = 3,
) -> MagicMock:
    t = MagicMock()
    t.jobID = uuid.uuid4()
    t.task_type = task_type
    t.status = status
    t.error_message = error_message
    t.retry_count = retry_count
    t.max_retries = max_retries
    t.timestamp = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t.finished_at = datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
    return t


# ---------------------------------------------------------------------------
# Import the handler coroutine under test.
# ---------------------------------------------------------------------------

from dynastore.extensions.admin.admin_service import AdminService  # noqa: E402

_handler = AdminService.get_catalog_provisioning_view


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"
_TASKS_MODULE = "dynastore.modules.tasks.tasks_module.list_tasks"
_MANAGED_TX = "dynastore.modules.db_config.query_executor.managed_transaction"


def _make_catalogs_mock(catalog: Any, physical_schema: str | None = "s_test") -> MagicMock:
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=catalog)
    svc.resolve_physical_schema = AsyncMock(return_value=physical_schema)
    return svc


def _make_db_mock() -> MagicMock:
    db = MagicMock()
    db.engine = MagicMock()
    return db


@asynccontextmanager
async def _fake_managed_tx(_engine):
    """Async context manager stub — yields a sentinel connection object."""
    yield MagicMock()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ready_catalog_no_tasks_returns_task_none():
    """A catalog with provisioning_status='ready' and no provision tasks
    returns a CatalogProvisioningView with task=None."""
    catalog = _make_catalog("cat-a", provisioning_status="ready")
    catalogs_svc = _make_catalogs_mock(catalog, physical_schema="s_cat_a")
    db_mock = _make_db_mock()

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is DatabaseProtocol:
            return db_mock
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MANAGED_TX, side_effect=_fake_managed_tx), \
         patch(_TASKS_MODULE, new=AsyncMock(return_value=[])):
        result = await _handler(catalog_id="cat-a")

    assert result.catalog_id == "cat-a"
    assert result.provisioning_status == "ready"
    assert result.physical_schema == "s_cat_a"
    assert result.task is None


@pytest.mark.asyncio
async def test_provisioning_catalog_with_running_task():
    """A catalog in 'provisioning' state with a RUNNING task surfaces the
    task fields including retry count and timestamps."""
    catalog = _make_catalog("cat-b", provisioning_status="provisioning")
    catalogs_svc = _make_catalogs_mock(catalog, physical_schema="s_cat_b")
    db_mock = _make_db_mock()
    running_task = _make_task(
        status=TaskStatusEnum.ACTIVE,
        retry_count=1,
        max_retries=3,
    )

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is DatabaseProtocol:
            return db_mock
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MANAGED_TX, side_effect=_fake_managed_tx), \
         patch(_TASKS_MODULE, new=AsyncMock(return_value=[running_task])):
        result = await _handler(catalog_id="cat-b")

    assert result.provisioning_status == "provisioning"
    assert result.task is not None
    assert result.task.status == TaskStatusEnum.ACTIVE.value
    assert result.task.retry_count == 1
    assert result.task.max_retries == 3
    assert result.task.error_message is None
    assert result.task.task_id == running_task.jobID


@pytest.mark.asyncio
async def test_failed_catalog_with_failed_task_exposes_error_message():
    """A catalog in 'failed' state with a FAILED task surfaces error_message."""
    catalog = _make_catalog("cat-c", provisioning_status="failed")
    catalogs_svc = _make_catalogs_mock(catalog, physical_schema="s_cat_c")
    db_mock = _make_db_mock()
    failed_task = _make_task(
        status=TaskStatusEnum.FAILED,
        error_message="Bucket name collision on GCS.",
        retry_count=3,
    )

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is DatabaseProtocol:
            return db_mock
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MANAGED_TX, side_effect=_fake_managed_tx), \
         patch(_TASKS_MODULE, new=AsyncMock(return_value=[failed_task])):
        result = await _handler(catalog_id="cat-c")

    assert result.provisioning_status == "failed"
    assert result.task is not None
    assert result.task.status == TaskStatusEnum.FAILED.value
    assert result.task.error_message == "Bucket name collision on GCS."


@pytest.mark.asyncio
async def test_unknown_catalog_raises_404():
    """An unknown catalog_id causes the handler to raise HTTP 404."""
    catalogs_svc = MagicMock()
    catalogs_svc.get_catalog_model = AsyncMock(return_value=None)

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        if cls is CatalogsProtocol:
            return catalogs_svc
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        with pytest.raises(HTTPException) as exc_info:
            await _handler(catalog_id="nonexistent")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_catalogs_service_unavailable_raises_503():
    """When CatalogsProtocol is not registered the handler raises HTTP 503."""
    def _get_proto(cls):
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        with pytest.raises(HTTPException) as exc_info:
            await _handler(catalog_id="any-catalog")

    assert exc_info.value.status_code == 503
