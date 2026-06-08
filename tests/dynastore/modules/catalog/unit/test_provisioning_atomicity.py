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

"""Unit tests for provisioning atomicity guard (#1847).

Defense-in-depth: ``create_physical_collection`` must never silently return
when ``get_driver("WRITE")`` raises ``ValueError``.  Doing so would leave a
``physical_table`` pin committed (by a prior ``set_physical_table`` call in
the same request) without the physical table existing — the divergent state
described in #1847.

The fix (``raise`` instead of ``return``) lets the caller's
``managed_transaction`` roll back the pin atomically with the failed table
creation, so the two artefacts can never diverge.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch


def _make_catalog_service():
    """Construct a minimal CatalogService without running __init__."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    return svc


class TestCreatePhysicalCollectionAtomicityGuard:
    """``create_physical_collection`` must re-raise when no WRITE driver exists."""

    @pytest.mark.asyncio
    async def test_no_write_driver_raises_value_error(self):
        """When ``get_driver("WRITE")`` raises ValueError, the exception
        propagates so the caller's transaction can roll back any prior
        ``set_physical_table`` pin.
        """
        svc = _make_catalog_service()
        conn = AsyncMock()

        with patch(
            "dynastore.modules.storage.router.get_driver",
            side_effect=ValueError("no WRITE driver registered"),
        ):
            with pytest.raises(ValueError, match="no WRITE driver"):
                await svc.create_physical_collection(
                    conn,
                    schema="public",
                    catalog_id="cat1",
                    collection_id="col1",
                    physical_table="t_should_not_be_created",
                )

    @pytest.mark.asyncio
    async def test_no_write_driver_ensure_storage_not_called(self):
        """``ensure_storage`` must never be called when driver lookup fails."""
        svc = _make_catalog_service()
        conn = AsyncMock()
        mock_driver = AsyncMock()

        with patch(
            "dynastore.modules.storage.router.get_driver",
            side_effect=ValueError("no WRITE driver"),
        ) as mock_get_driver:
            with pytest.raises(ValueError):
                await svc.create_physical_collection(
                    conn,
                    schema="public",
                    catalog_id="cat1",
                    collection_id="col1",
                    physical_table="t_phantom",
                )
            mock_driver.ensure_storage.assert_not_called()
            mock_get_driver.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_calls_ensure_storage(self):
        """When a driver is found, ``ensure_storage`` is called with the
        correct arguments — existing behaviour must not regress.
        """
        svc = _make_catalog_service()
        conn = AsyncMock()
        mock_driver = AsyncMock()

        with patch(
            "dynastore.modules.storage.router.get_driver",
            return_value=mock_driver,
        ):
            await svc.create_physical_collection(
                conn,
                schema="public",
                catalog_id="cat1",
                collection_id="col1",
                physical_table="t_real",
                layer_config={"key": "value"},
            )

        mock_driver.ensure_storage.assert_awaited_once_with(
            "cat1",
            "col1",
            physical_table="t_real",
            layer_config={"key": "value"},
            db_resource=conn,
        )
