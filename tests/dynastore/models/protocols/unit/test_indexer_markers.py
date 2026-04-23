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
    ItemIndexer,
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
