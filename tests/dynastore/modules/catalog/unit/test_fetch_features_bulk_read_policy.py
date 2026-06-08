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

"""Unit tests pinning ItemsReadPolicy threading on the write read-back path.

After a distributed write commits, ``fetch_features_bulk`` re-loads the freshly
written rows on a clean connection. This read-back must honour the same
read-time wire-shape contract (``feature_type.expose`` merge and
``external_id_as_feature_id``) as the canonical read paths — otherwise the 201
/ 207 response body for a create silently differs from a subsequent GET
(issue #1076).

These tests pin that ``fetch_features_bulk`` resolves the collection's
``ItemsReadPolicy`` once and threads it into both the ``QueryOptimizer`` (SQL
``external_id``-as-id aliasing) and ``map_row_to_feature`` (expose merge).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.read_policy import ItemsReadPolicy
from dynastore.modules.storage.computed_fields import FeatureType


@pytest.mark.asyncio
async def test_fetch_features_bulk_threads_read_policy() -> None:
    """When catalog/collection are supplied, the resolved read policy must be
    forwarded into ``map_row_to_feature`` and the ``QueryOptimizer``."""
    svc = ItemService(engine=MagicMock())

    policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
    svc._resolve_read_policy = AsyncMock(return_value=policy)

    captured: dict = {}

    def _capture_map(row, col_config, read_policy=None):
        captured.setdefault("policies", []).append(read_policy)
        return MagicMock()

    svc.map_row_to_feature = _capture_map  # type: ignore[method-assign]

    col_config = ItemsPostgresqlDriverConfig()

    # Stub the SQL build + execution so no DB is touched.
    fake_optimizer = MagicMock()
    fake_optimizer.build_optimized_query.return_value = ("SELECT 1", {})

    with patch(
        "dynastore.modules.catalog.item_distributed.QueryOptimizer",
        return_value=fake_optimizer,
    ) as opt_cls, patch(
        "dynastore.modules.catalog.item_distributed.DQLQuery"
    ) as dql_cls:
        dql_cls.return_value.execute = AsyncMock(
            return_value=[{"geoid": "g1"}, {"geoid": "g2"}]
        )
        out = await svc.fetch_features_bulk(
            MagicMock(),  # conn
            "schema",
            "hub",
            ["g1", "g2"],
            col_config,
            catalog_id="cat",
            collection_id="col",
        )

    svc._resolve_read_policy.assert_awaited_once_with("cat", "col")
    # QueryOptimizer constructed with the resolved policy (SQL aliasing path).
    assert opt_cls.call_args.kwargs.get("read_policy") is policy
    # Every row mapped with the resolved policy (expose-merge path).
    assert len(out) == 2
    assert captured["policies"] == [policy, policy]


@pytest.mark.asyncio
async def test_fetch_features_bulk_no_ids_returns_empty() -> None:
    """Empty geoid batch short-circuits without resolving a policy."""
    svc = ItemService(engine=MagicMock())
    svc._resolve_read_policy = AsyncMock(return_value=None)

    out = await svc.fetch_features_bulk(
        MagicMock(), "schema", "hub", [], ItemsPostgresqlDriverConfig(),
        catalog_id="cat", collection_id="col",
    )
    assert out == []
    svc._resolve_read_policy.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_features_bulk_skips_resolution_without_ctx() -> None:
    """Back-compat: callers that omit catalog/collection get the default wire
    shape (read_policy=None) and no resolution attempt."""
    svc = ItemService(engine=MagicMock())
    svc._resolve_read_policy = AsyncMock(return_value=None)

    captured: dict = {}

    def _capture_map(row, col_config, read_policy=None):
        captured["read_policy"] = read_policy
        return MagicMock()

    svc.map_row_to_feature = _capture_map  # type: ignore[method-assign]

    fake_optimizer = MagicMock()
    fake_optimizer.build_optimized_query.return_value = ("SELECT 1", {})

    with patch(
        "dynastore.modules.catalog.item_distributed.QueryOptimizer",
        return_value=fake_optimizer,
    ), patch(
        "dynastore.modules.catalog.item_distributed.DQLQuery"
    ) as dql_cls:
        dql_cls.return_value.execute = AsyncMock(return_value=[{"geoid": "g1"}])
        await svc.fetch_features_bulk(
            MagicMock(), "schema", "hub", ["g1"], ItemsPostgresqlDriverConfig(),
        )

    svc._resolve_read_policy.assert_not_awaited()
    assert captured["read_policy"] is None


def test_catalog_service_map_row_to_feature_delegates_read_policy() -> None:
    """``CatalogService.map_row_to_feature`` is a thin delegate to the item
    service; it must forward ``read_policy`` so OGC callers going through the
    CatalogsProtocol surface honour the wire-shape contract."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    captured: dict = {}

    class _Item:
        def map_row_to_feature(self, row, col_config, lang="en", read_policy=None):
            captured["read_policy"] = read_policy
            captured["lang"] = lang
            return MagicMock()

    svc = CatalogService(item_service=_Item())  # type: ignore[arg-type]
    policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
    svc.map_row_to_feature({"geoid": "g1"}, MagicMock(), lang="fr", read_policy=policy)

    assert captured["read_policy"] is policy
    assert captured["lang"] == "fr"


def test_catalog_service_map_row_to_feature_default_read_policy() -> None:
    """Omitting ``read_policy`` delegates ``None`` (default wire shape)."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    captured: dict = {}

    class _Item:
        def map_row_to_feature(self, row, col_config, lang="en", read_policy=None):
            captured["read_policy"] = read_policy
            return MagicMock()

    svc = CatalogService(item_service=_Item())  # type: ignore[arg-type]
    svc.map_row_to_feature({"geoid": "g1"}, MagicMock())
    assert captured["read_policy"] is None
