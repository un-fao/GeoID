"""Unit tests for the catalog-wide deny-all-except-lookup bundle generator (#284).

Asserts the generated bundle:
- DENYs every listed OGC + platform service prefix (catalog-scoped),
- includes the two ALLOW carve-outs with the right conditions,
- is catalog-scoped throughout,
- round-trips through the Policy / Condition Pydantic models (no DB).
"""
from dynastore.extensions.geoid.deny_bundle import (
    DEFAULT_DENY_SERVICE_PREFIXES,
    build_lookup_only_deny_bundle,
)
from dynastore.models.auth import Policy

CATALOG = "democat"
WRITE_COLLS = ["coll1", "coll2"]


def _bundle():
    return build_lookup_only_deny_bundle(CATALOG, write_collections=WRITE_COLLS)


def test_every_service_prefix_has_a_catalog_scoped_deny():
    """Each listed prefix produces a DENY policy covering both the bare
    surface and the /{service}/catalogs/{cat}/... variant."""
    policies = _bundle()
    by_effect_deny = [p for p in policies if p.effect == "DENY"]

    # exactly one DENY per service prefix
    assert len(by_effect_deny) == len(DEFAULT_DENY_SERVICE_PREFIXES)

    for prefix in DEFAULT_DENY_SERVICE_PREFIXES:
        matching = [
            p for p in by_effect_deny if any(r.startswith(f"/{prefix}") for r in p.resources)
        ]
        assert matching, f"no DENY policy for service prefix /{prefix}"
        p = matching[0]
        # bare surface + catalog-scoped variant both present
        assert any(r == rf"/{prefix}(/.*)?" for r in p.resources)
        assert any(
            f"/{prefix}/catalogs/{CATALOG}" in r for r in p.resources
        ), f"DENY for /{prefix} not catalog-scoped"
        # blocks every method
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            assert method in p.actions


def test_expected_service_prefixes_present():
    """The default prefix set matches the issue's listed services exactly."""
    expected = {
        "stac",
        "features",
        "coverages",
        "records",
        "tiles",
        "processes",
        "edr",
        "maps",
        "styles",
        "dggs",
        "consys",
        "moving_features",
    }
    assert set(DEFAULT_DENY_SERVICE_PREFIXES) == expected


def test_lookup_search_allow_carveout():
    """ALLOW GET|POST /search + /search/catalogs/{cat} gated by lookup_only_search."""
    policies = _bundle()
    allow = [p for p in policies if p.effect == "ALLOW" and "lookup_search" in p.id]
    assert len(allow) == 1
    p = allow[0]
    assert set(p.actions) == {"GET", "POST"}
    assert any(r == r"/search(/.*)?" for r in p.resources)
    assert any(f"/search/catalogs/{CATALOG}" in r for r in p.resources)
    assert [c.type for c in p.conditions] == ["lookup_only_search"]
    # ALLOW must out-rank the DENY baseline (deny-precedence on ties)
    deny = next(p for p in policies if p.effect == "DENY")
    assert p.priority > deny.priority


def test_collection_write_allow_carveout():
    """ALLOW POST .../features/.../collections/(coll1|coll2)/items gated by
    collection_write_anonymous_allowed, scoped to named collections + catalog."""
    policies = _bundle()
    allow = [p for p in policies if p.effect == "ALLOW" and "collection_write" in p.id]
    assert len(allow) == 1
    p = allow[0]
    assert p.actions == ["POST"]
    assert len(p.resources) == 1
    res = p.resources[0]
    assert f"/features/catalogs/{CATALOG}/collections/" in res
    assert "coll1" in res and "coll2" in res
    assert res.endswith("/items")
    assert [c.type for c in p.conditions] == ["collection_write_anonymous_allowed"]
    assert p.priority > 0


def test_write_carveout_omitted_when_no_collections():
    """No write ALLOW emitted when write_collections is empty."""
    policies = build_lookup_only_deny_bundle(CATALOG)
    assert not [p for p in policies if "collection_write" in p.id]
    # the lookup-search ALLOW is still present
    assert [p for p in policies if "lookup_search" in p.id]


def test_bundle_is_catalog_scoped_via_ids():
    """Policy ids embed the catalog so re-pushing per catalog is namespaced."""
    policies = _bundle()
    assert policies
    for p in policies:
        assert CATALOG in p.id


def test_bundle_round_trips_through_pydantic():
    """Every policy serializes and re-validates through the Policy model
    (the shape POST /admin/policies?catalog_id=... accepts)."""
    policies = _bundle()
    for p in policies:
        dumped = p.model_dump()
        # required PolicyCreate-shaped keys present
        for key in ("id", "actions", "resources", "effect", "priority", "conditions"):
            assert key in dumped
        revalidated = Policy.model_validate(dumped)
        assert revalidated.id == p.id
        assert revalidated.effect == p.effect
        assert revalidated.actions == p.actions
        assert revalidated.resources == p.resources
        assert [c.type for c in revalidated.conditions] == [c.type for c in p.conditions]


def test_resources_compile_as_regex():
    """All resource patterns compile (model_post_init builds re.Pattern caches)."""
    for p in _bundle():
        # matches_resource exercises the compiled patterns without raising
        p.matches_resource("/stac/catalogs/demo-cat/collections")


def test_empty_catalog_id_rejected():
    import pytest

    with pytest.raises(ValueError):
        build_lookup_only_deny_bundle("")
