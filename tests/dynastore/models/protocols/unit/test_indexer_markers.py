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

"""Per-tier indexer marker discrimination.

Pins the contract that the four marker Protocols
(``CatalogIndexer``, ``CollectionIndexer``, ``AssetIndexer``, ``ItemIndexer``)
discriminate by ``ClassVar[bool]`` opt-in flag and that the existing ES
drivers correctly self-declare their tiers.

A driver indexing multiple tiers opts in to multiple markers; a driver
indexing none of them satisfies none.  The split-by-tier is independent
of the data/metadata distinction — both are indexable.
"""

from __future__ import annotations

from typing import ClassVar

from dynastore.models.protocols.indexer import (
    AssetIndexer,
    CatalogIndexer,
    CollectionIndexer,
    ItemAssetIndexer,
    ItemIndexer,
    PlatformAssetIndexer,
)


# ---------------------------------------------------------------------------
# Marker discrimination — minimal stubs
# ---------------------------------------------------------------------------


def test_marker_requires_opt_in_flag():
    """A class without the marker's ClassVar flag is NOT a marker member."""

    class _NoFlag:
        pass

    assert not isinstance(_NoFlag(), CatalogIndexer)
    assert not isinstance(_NoFlag(), CollectionIndexer)
    assert not isinstance(_NoFlag(), AssetIndexer)
    assert not isinstance(_NoFlag(), ItemIndexer)
    assert not isinstance(_NoFlag(), ItemAssetIndexer)
    assert not isinstance(_NoFlag(), PlatformAssetIndexer)


def test_item_asset_marker_requires_opt_in_flag():
    """``is_item_asset_indexer = True`` opts in to :class:`ItemAssetIndexer`,
    nothing else.  No implementer ships in this PR; this test pins the
    contract so a future opt-in surfaces immediately.
    """

    class _ItemAssetOnly:
        is_item_asset_indexer: ClassVar[bool] = True

    obj = _ItemAssetOnly()
    assert isinstance(obj, ItemAssetIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, PlatformAssetIndexer)
    assert not isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, ItemIndexer)


def test_platform_asset_marker_requires_opt_in_flag():
    """``is_platform_asset_indexer = True`` opts in to
    :class:`PlatformAssetIndexer` only.  No implementer ships in this PR.
    """

    class _PlatformAssetOnly:
        is_platform_asset_indexer: ClassVar[bool] = True

    obj = _PlatformAssetOnly()
    assert isinstance(obj, PlatformAssetIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemAssetIndexer)
    assert not isinstance(obj, CatalogIndexer)


def test_existing_asset_es_driver_opts_into_AssetIndexer_only():
    """``AssetElasticsearchDriver`` covers catalog + collection assets via
    its per-catalog index — it MUST opt into :class:`AssetIndexer` only,
    NOT into :class:`ItemAssetIndexer` or :class:`PlatformAssetIndexer`.
    Surfaces accidental future opt-ins that haven't yet shipped the
    item-asset / platform-asset write paths.
    """
    from dynastore.modules.storage.drivers.elasticsearch import (
        AssetElasticsearchDriver,
    )

    obj = AssetElasticsearchDriver()
    assert isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemAssetIndexer)
    assert not isinstance(obj, PlatformAssetIndexer)


def test_single_tier_opt_in():
    """Setting one ClassVar flag opts in to that tier only."""

    class _CatOnly:
        is_catalog_indexer: ClassVar[bool] = True

    obj = _CatOnly()
    assert isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)


def test_multi_tier_opt_in():
    """A driver indexing multiple tiers opts in to multiple markers."""

    class _CatAndCol:
        is_catalog_indexer: ClassVar[bool] = True
        is_collection_indexer: ClassVar[bool] = True

    obj = _CatAndCol()
    assert isinstance(obj, CatalogIndexer)
    assert isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)


# ---------------------------------------------------------------------------
# Existing ES drivers — self-declared tiers
# ---------------------------------------------------------------------------


def test_catalog_es_driver_indexes_catalog_only():
    """``CatalogElasticsearchDriver`` indexes ONE tier — catalog metadata,
    keyed by ``catalog_id``.  It opts in to :class:`CatalogIndexer` only.
    """
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriver,
    )

    obj = CatalogElasticsearchDriver()
    assert isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)
    assert not isinstance(obj, ItemAssetIndexer)
    assert not isinstance(obj, PlatformAssetIndexer)


def test_collection_es_driver_indexes_collection_only():
    """``CollectionElasticsearchDriver`` indexes ONE tier — collection
    metadata, keyed by ``(catalog_id, collection_id)``.  It opts in to
    :class:`CollectionIndexer` only.  Catalog-tier indexing is a
    separate driver class (NEW; not part of the catch-all rename).
    """
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriver,
    )

    obj = CollectionElasticsearchDriver()
    assert isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)


def test_items_es_driver_indexes_items_only():
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    obj = ItemsElasticsearchDriver()
    assert isinstance(obj, ItemIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)


def test_asset_es_driver_indexes_assets_only():
    from dynastore.modules.storage.drivers.elasticsearch import (
        AssetElasticsearchDriver,
    )

    obj = AssetElasticsearchDriver()
    assert isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)
    assert not isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)
