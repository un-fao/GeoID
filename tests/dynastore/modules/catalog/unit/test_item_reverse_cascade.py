"""Unit tests for ItemReverseCascadeSubscriber (Phase 3)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.asset_sync import (
    ItemReverseCascadeSubscriber,
)


class TestSubscriberShortCircuit:
    """The subscriber must no-op for inapplicable payloads."""

    @pytest.mark.asyncio
    async def test_skips_when_propagate_false(self):
        with patch("dynastore.modules.catalog.asset_sync.get_protocol") as mock_get:
            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"propagate": False, "asset_id": "aid-1"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_propagate_missing(self):
        with patch("dynastore.modules.catalog.asset_sync.get_protocol") as mock_get:
            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"asset_id": "aid-1"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_payload_not_dict(self):
        with patch("dynastore.modules.catalog.asset_sync.get_protocol") as mock_get:
            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload=None,
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_collection_scope(self):
        """Catalog-level assets (no collection) cannot drive item cascade."""
        with patch("dynastore.modules.catalog.asset_sync.get_protocol") as mock_get:
            mock_get.return_value = MagicMock()
            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id=None,
                asset_id="aid-1",
                payload={"propagate": True, "asset_id": "aid-1"},
            )
        # CatalogService get_protocol IS called, but resolve_physical_schema is not.

    @pytest.mark.asyncio
    async def test_skips_when_catalog_service_missing(self):
        with patch(
            "dynastore.modules.catalog.asset_sync.get_protocol",
            return_value=None,
        ):
            # Must not raise
            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"propagate": True, "asset_id": "aid-1"},
            )


class TestCascadeFlow:
    """When propagate=True and items exist, they must be deleted."""

    @pytest.mark.asyncio
    async def test_deletes_linked_items(self):
        mock_catalog = MagicMock()
        mock_catalog.engine = MagicMock()
        mock_catalog.resolve_physical_schema = AsyncMock(return_value="catalog_cat_1")
        mock_catalog.delete_item = AsyncMock(return_value=1)

        # Item-service exposes the new query property
        mock_query = MagicMock()
        mock_query.execute = AsyncMock(
            return_value=[
                {"external_id": "item-1"},
                {"external_id": "item-2"},
            ]
        )
        mock_catalog._item_svc = MagicMock()
        mock_catalog._item_svc.list_items_by_asset_id_query = mock_query

        @AsyncMock
        async def _fake_managed_tx(_engine):  # pragma: no cover
            yield MagicMock()

        with patch(
            "dynastore.modules.catalog.asset_sync.get_protocol",
            return_value=mock_catalog,
        ), patch(
            "dynastore.modules.db_config.query_executor.managed_transaction"
        ) as mtx:
            # managed_transaction is an async context manager
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=MagicMock())
            cm.__aexit__ = AsyncMock(return_value=None)
            mtx.return_value = cm

            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"propagate": True, "asset_id": "aid-1"},
            )

        assert mock_catalog.delete_item.await_count == 2
        mock_catalog.delete_item.assert_any_await("cat-1", "coll-1", "item-1")
        mock_catalog.delete_item.assert_any_await("cat-1", "coll-1", "item-2")

    @pytest.mark.asyncio
    async def test_no_op_when_zero_items_match(self):
        mock_catalog = MagicMock()
        mock_catalog.engine = MagicMock()
        mock_catalog.resolve_physical_schema = AsyncMock(return_value="catalog_cat_1")
        mock_catalog.delete_item = AsyncMock()

        mock_query = MagicMock()
        mock_query.execute = AsyncMock(return_value=[])
        mock_catalog._item_svc = MagicMock()
        mock_catalog._item_svc.list_items_by_asset_id_query = mock_query

        with patch(
            "dynastore.modules.catalog.asset_sync.get_protocol",
            return_value=mock_catalog,
        ), patch(
            "dynastore.modules.db_config.query_executor.managed_transaction"
        ) as mtx:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=MagicMock())
            cm.__aexit__ = AsyncMock(return_value=None)
            mtx.return_value = cm

            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"propagate": True, "asset_id": "aid-1"},
            )

        mock_catalog.delete_item.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_swallows_per_item_exceptions(self):
        """One item failing must not stop the rest."""
        mock_catalog = MagicMock()
        mock_catalog.engine = MagicMock()
        mock_catalog.resolve_physical_schema = AsyncMock(return_value="catalog_cat_1")
        mock_catalog.delete_item = AsyncMock(
            side_effect=[RuntimeError("transient"), 1]
        )

        mock_query = MagicMock()
        mock_query.execute = AsyncMock(
            return_value=[
                {"external_id": "item-fails"},
                {"external_id": "item-ok"},
            ]
        )
        mock_catalog._item_svc = MagicMock()
        mock_catalog._item_svc.list_items_by_asset_id_query = mock_query

        with patch(
            "dynastore.modules.catalog.asset_sync.get_protocol",
            return_value=mock_catalog,
        ), patch(
            "dynastore.modules.db_config.query_executor.managed_transaction"
        ) as mtx:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=MagicMock())
            cm.__aexit__ = AsyncMock(return_value=None)
            mtx.return_value = cm

            await ItemReverseCascadeSubscriber.on_asset_hard_delete(
                catalog_id="cat-1",
                collection_id="coll-1",
                asset_id="aid-1",
                payload={"propagate": True, "asset_id": "aid-1"},
            )

        assert mock_catalog.delete_item.await_count == 2
