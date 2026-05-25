"""Pin routing defaults so a future edit can't silently flip ES back
from OUTBOX without explicit test update. PG must remain FATAL —
non-negotiable for authoritative writes."""
from __future__ import annotations


def test_default_pg_write_is_sync_fatal():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    pg = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert pg.on_failure == FailurePolicy.FATAL
    assert pg.write_mode == WriteMode.SYNC


def test_default_es_write_is_async_outbox():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    es = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_elasticsearch_driver"
    )
    assert es.on_failure == FailurePolicy.OUTBOX
    assert es.write_mode == WriteMode.ASYNC


def test_default_read_routing_unchanged():
    """Sanity guard — Task 11 only touches WRITE; READ entries
    (ES geometry_simplified primary, PG geometry_exact) must remain."""
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    read = cfg.operations[Operation.READ]
    es_read = next(e for e in read if e.driver_ref == "items_elasticsearch_driver")
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert "geometry_simplified" in es_read.hints
    assert "geometry_exact" in pg_read.hints
    # READ defaults retain their existing failure semantics — flag if changed.
    assert pg_read.on_failure == FailurePolicy.FATAL


def test_default_items_search_declares_op_appropriate_hints():
    """SEARCH entries declare the search-flavour hints each driver serves so a
    resolved config is self-documenting. ES carries the search engine flavours
    (search/fulltext/attribute_filter/...); PG carries the relational ones
    (attribute_filter/group_by/geometry_exact/...)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "items_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "items_postgresql_driver")
    # both serve the common filter/sort flavours
    for h in (Hint.ATTRIBUTE_FILTER, Hint.SPATIAL_FILTER, Hint.SORT):
        assert h in es.hints and h in pg.hints
    # engine-only vs relational-only
    assert Hint.FULLTEXT in es.hints and Hint.SEARCH in es.hints
    assert Hint.GROUP_BY in pg.hints
    assert Hint.GEOMETRY_SIMPLIFIED in es.hints
    assert Hint.GEOMETRY_EXACT in pg.hints


def test_default_items_search_excludes_read_only_hints():
    """``tiles`` (and other non-search hints) only make sense on READ — they
    must NOT appear in SEARCH entries."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    for e in cfg.operations[Operation.SEARCH]:
        assert Hint.TILES not in e.hints
        assert Hint.WRITE not in e.hints
        assert Hint.METADATA not in e.hints
        assert Hint.JOIN not in e.hints
    # tiles is still declared where it belongs: the PG READ entry
    read = cfg.operations[Operation.READ]
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert Hint.TILES in pg_read.hints


def test_default_items_filtered_search_resolves_es_first():
    """A filtered/sorted search routes to ES first (the search engine): the
    declared ES search surface is a strict superset-by-size of PG's, so the
    best-overlap matcher's longest-effective tiebreak ranks ES above PG for any
    flavour both serve. Unfiltered search keeps declared order (ES then PG)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "items_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "items_postgresql_driver")

    def resolve(requested):
        if not requested:
            return [e.driver_ref for e in search]
        matched = [
            (i, e) for i, e in enumerate(search)
            if requested.issubset(frozenset(e.hints))
        ]
        matched.sort(key=lambda t: (-len(t[1].hints), t[0]))
        return [e.driver_ref for _, e in matched]

    assert len(es.hints) > len(pg.hints)  # ES wins the longest-effective tiebreak
    assert resolve(frozenset()) == [
        "items_elasticsearch_driver", "items_postgresql_driver",
    ]
    for flavour in (Hint.ATTRIBUTE_FILTER, Hint.SPATIAL_FILTER, Hint.SORT):
        assert resolve(frozenset({flavour}))[0] == "items_elasticsearch_driver"
    # group_by is relational-only → PG even though ES is listed first
    assert resolve(frozenset({Hint.GROUP_BY})) == ["items_postgresql_driver"]


def test_collection_routing_default_write_is_pg_fatal():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation, WriteMode,
    )
    cfg = CollectionRoutingConfig()
    write = cfg.operations[Operation.WRITE]
    assert [e.driver_ref for e in write] == ["collection_postgresql_driver"]
    assert write[0].on_failure == FailurePolicy.FATAL
    assert write[0].write_mode == WriteMode.SYNC


def test_collection_routing_default_read_is_pg_primary():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    read = cfg.operations[Operation.READ]
    assert [e.driver_ref for e in read] == ["collection_postgresql_driver"]
    assert read[0].on_failure == FailurePolicy.FATAL


def test_collection_routing_default_index_has_no_hardcoded_es_hop():
    """The ES secondary-index hop is NOT hard-coded in the code default
    (#1069 / #1073).

    A PG-only deployment (no ES CollectionIndexer registered, no preset
    applied) must get NO secondary-index WRITE entry — otherwise a plain
    collection create enqueues an OUTBOX row into tasks.tasks that nothing
    will ever drain, which poisons the create transaction when the outbox
    table is absent. The ES secondary-index hop (ASYNC + OUTBOX) is supplied
    at validation time by ``_self_register_indexers_into`` when an ES driver
    is registered (see test_collection_routing_validator_augments_write_index_and_search)
    and by the routing presets (see test_preset_public_catalog)."""
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, secondary_index_entries,
    )
    cfg = CollectionRoutingConfig()
    index = secondary_index_entries(cfg.operations)
    assert "collection_elasticsearch_driver" not in {e.driver_ref for e in index}


def test_collection_routing_default_search_is_es_first_pg_fallback():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    refs = [e.driver_ref for e in search]
    assert refs[0] == "collection_elasticsearch_driver"
    assert "collection_postgresql_driver" in refs


def test_collection_routing_default_search_carries_geometry_hints():
    """SEARCH entries declare which geometry precision they serve so a
    consumer can route to PG via hint='geometry_exact'. ES carries
    GEOMETRY_SIMPLIFIED (fast, lossy); PG carries GEOMETRY_EXACT (full WKB).
    Mirrors ItemsRoutingConfig READ defaults."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "collection_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "collection_postgresql_driver")
    assert Hint.GEOMETRY_SIMPLIFIED in es.hints
    assert Hint.GEOMETRY_EXACT in pg.hints


def test_catalog_routing_default_refs_are_registered():
    """The default WRITE/READ entries must reference an actually-registered
    driver_ref. catalog_core_postgresql_driver / catalog_stac_postgresql_driver
    were never registered as entry-points — the registered wrapper is
    catalog_postgresql_driver."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )
    cfg = CatalogRoutingConfig()
    write_refs = [e.driver_ref for e in cfg.operations[Operation.WRITE]]
    read_refs = [e.driver_ref for e in cfg.operations[Operation.READ]]
    assert write_refs == ["catalog_postgresql_driver"]
    assert read_refs == ["catalog_postgresql_driver"]
    for ref in write_refs + read_refs:
        assert ref not in (
            "catalog_core_postgresql_driver",
            "catalog_stac_postgresql_driver",
        ), f"{ref} is not a registered entry-point"


def test_catalog_routing_default_write_is_fatal():
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CatalogRoutingConfig()
    assert cfg.operations[Operation.WRITE][0].on_failure == FailurePolicy.FATAL
