"""Cycle E.2.b — smoke tests for ``CollectionElasticsearchPrivateDriver``.

Pins the structural invariants of the new driver class without
exercising the live ES client (those tests live under
``tests/dynastore/modules/elasticsearch/`` and run against an
in-cluster fixture):

- Distinct ``indexer_id`` so the dispatcher routes to the right driver.
- ``auto_register_for_routing`` is empty — operators must explicitly
  pin this driver in :class:`CollectionRoutingConfig`; auto-default
  augmentation must NEVER reach for it.
- Per-tenant index name resolution via
  ``get_tenant_collections_private_index`` matches the convention.
- ``location`` returns the per-tenant index path (not the shared
  ``{prefix}-collections`` of the public driver).
"""
from __future__ import annotations

from unittest.mock import patch

from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
)
from dynastore.modules.elasticsearch.mappings import (
    get_tenant_collections_private_index,
)
from dynastore.modules.storage.drivers.elasticsearch_private import (
    CollectionElasticsearchPrivateDriver,
)


def test_indexer_id_is_distinct():
    assert (
        CollectionElasticsearchPrivateDriver.indexer_id
        == "collection_elasticsearch_private_driver"
    )
    # The public driver doesn't declare an ``indexer_id`` ClassVar
    # (it's identified via class-name → snake-case in the dispatcher),
    # so the comparison check is just an explicit-value pin here.


def test_auto_register_for_routing_is_empty():
    """Operators must explicitly pin the private driver — never
    auto-injected, even into ``CollectionRoutingConfig.operations[INDEX]``."""
    assert CollectionElasticsearchPrivateDriver.auto_register_for_routing == frozenset()


def test_subclasses_public_driver():
    """The private driver inherits the public ``CollectionStore`` surface
    so we get capabilities, doc enrichment helpers, and the
    is_collection_indexer marker for free."""
    assert issubclass(
        CollectionElasticsearchPrivateDriver, CollectionElasticsearchDriver,
    )


def test_per_tenant_index_name():
    """Index name follows the per-catalog convention."""
    expected = get_tenant_collections_private_index("dynastore", "cat-a")
    assert expected == "dynastore-cat-a-collections-private"


def test_index_name_resolution_uses_per_tenant_helper():
    driver = CollectionElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        assert driver._private_index("cat-a") == "dynastore-cat-a-collections-private"


def test_doc_id_is_bare_collection_id():
    """Tenant-scoped index → no catalog prefix needed in doc-id."""
    driver = CollectionElasticsearchPrivateDriver()
    assert driver._doc_id("col-a") == "col-a"


def test_location_returns_per_tenant_path():
    driver = CollectionElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        loc = driver.location("cat-a", "col-a")
    assert loc.backend == "elasticsearch_private"
    assert loc.canonical_uri == "es://dynastore-cat-a-collections-private"
    assert loc.identifiers["index"] == "dynastore-cat-a-collections-private"
    assert loc.identifiers["catalog_id"] == "cat-a"
    # No "routing" identifier — per-tenant index doesn't need shard routing.
    assert "routing" not in loc.identifiers
