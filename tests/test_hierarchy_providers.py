"""Tests for the pluggable HierarchyProvider layer.

Static and dimension-backed providers run in-process with no DB/HTTP.
Data-derived exercises config coercion only; SQL path is covered by
existing stac_hierarchy_queries tests.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.extensions.stac.hierarchy import (
    ChildrenPage,
    HierarchyProvider,
    HierarchyProviderConfig,
    HierarchyNode,
    get_hierarchy_provider,
)


TREE = [
    {"code": "AFR", "label": "Africa", "labels": {"en": "Africa", "fr": "Afrique"}, "parent_code": None, "level": 0},
    {"code": "EUR", "label": "Europe", "labels": {"en": "Europe", "fr": "Europe"}, "parent_code": None, "level": 0},
    {"code": "KEN", "label": "Kenya", "labels": {"en": "Kenya"}, "parent_code": "AFR", "level": 1},
    {"code": "TZA", "label": "Tanzania", "labels": {"en": "Tanzania"}, "parent_code": "AFR", "level": 1},
]


# ---------------------------------------------------------------------------
# Static generator
# ---------------------------------------------------------------------------

@pytest.fixture
def static_prov():
    cfg = HierarchyProviderConfig(
        kind="static", hierarchy_id="world", params={"tree": TREE},
    )
    return get_hierarchy_provider(cfg, None)


class TestStaticProvider:
    def test_protocol_compliance(self, static_prov):
        assert isinstance(static_prov, HierarchyProvider)
        assert static_prov.kind == "static"

    @pytest.mark.asyncio
    async def test_roots(self, static_prov):
        page = await static_prov.roots(None)
        codes = [m.code for m in page.members]
        assert set(codes) == {"AFR", "EUR"}
        assert page.number_matched == 2
        assert all(m.has_children or m.code == "EUR" for m in page.members)

    @pytest.mark.asyncio
    async def test_children(self, static_prov):
        page = await static_prov.children(None, "AFR")
        codes = [m.code for m in page.members]
        assert set(codes) == {"KEN", "TZA"}
        assert all(m.parent_code == "AFR" for m in page.members)

    @pytest.mark.asyncio
    async def test_ancestors_root_to_leaf(self, static_prov):
        chain = await static_prov.ancestors(None, "KEN")
        assert [n.code for n in chain] == ["AFR", "KEN"]

    @pytest.mark.asyncio
    async def test_has_children(self, static_prov):
        assert await static_prov.has_children(None, "AFR") is True
        assert await static_prov.has_children(None, "KEN") is False

    @pytest.mark.asyncio
    async def test_pagination(self, static_prov):
        page = await static_prov.children(None, "AFR", limit=1, offset=0)
        assert page.number_returned == 1
        assert page.number_matched == 2

    @pytest.mark.asyncio
    async def test_ancestors_cycle_terminates(self):
        cyclic = [
            {"code": "A", "label": "A", "parent_code": "B"},
            {"code": "B", "label": "B", "parent_code": "A"},
        ]
        gen = get_hierarchy_provider(
            HierarchyProviderConfig(kind="static", hierarchy_id="c", params={"tree": cyclic}),
            None,
        )
        chain = await gen.ancestors(None, "A")
        assert {n.code for n in chain} == {"A", "B"}


# ---------------------------------------------------------------------------
# Dimension-backed generator (stub provider — no ogc-dimensions required)
# ---------------------------------------------------------------------------

class _Member:
    def __init__(self, code, label, parent_code=None, level=None, has_children=False, labels=None):
        self.code = code
        self.label = label
        self.has_children = has_children
        self.extra = {"parent_code": parent_code, "level": level, "labels": labels or {}}


class _Page:
    def __init__(self, members):
        self.members = members
        self.number_matched = len(members)
        self.number_returned = len(members)


class _StubProvider:
    hierarchical = True

    def generate(self, a, b, limit=100, offset=0, parent=None):
        if parent is None:
            return _Page([_Member("AFR", "Africa", has_children=True, level=0)])
        return _Page([_Member("KEN", "Kenya", parent_code=parent, level=1)])

    def children(self, parent_code, limit=100, offset=0):
        return _Page([_Member("KEN", "Kenya", parent_code=parent_code, level=1)])

    def ancestors(self, code):
        return [{"code": "AFR", "label": "Africa", "parent_code": None, "level": 0},
                {"code": code, "label": code, "parent_code": "AFR", "level": 1}]

    def has_children(self, code):
        return code == "AFR"


class _FlatProvider:
    hierarchical = False


@pytest.fixture
def dim_prov():
    cfg = HierarchyProviderConfig(
        kind="dimension-backed", hierarchy_id="admin", dimension_id="admin-boundaries",
    )
    ctx = SimpleNamespace(provider=_StubProvider())
    return get_hierarchy_provider(cfg, ctx)


class TestDimensionBackedProvider:
    def test_rejects_non_hierarchical_provider(self):
        cfg = HierarchyProviderConfig(
            kind="dimension-backed", hierarchy_id="x", dimension_id="flat",
        )
        with pytest.raises(LookupError):
            get_hierarchy_provider(cfg, SimpleNamespace(provider=_FlatProvider()))

    @pytest.mark.asyncio
    async def test_roots_and_children(self, dim_prov):
        roots = await dim_prov.roots(None)
        assert [m.code for m in roots.members] == ["AFR"]
        children = await dim_prov.children(None, "AFR")
        assert [m.code for m in children.members] == ["KEN"]
        assert children.members[0].parent_code == "AFR"

    @pytest.mark.asyncio
    async def test_ancestors_pass_through(self, dim_prov):
        chain = await dim_prov.ancestors(None, "KEN")
        assert [n.code for n in chain] == ["AFR", "KEN"]

    @pytest.mark.asyncio
    async def test_has_children(self, dim_prov):
        assert await dim_prov.has_children(None, "AFR") is True
        assert await dim_prov.has_children(None, "KEN") is False


# ---------------------------------------------------------------------------
# Config coercion / validation
# ---------------------------------------------------------------------------

class TestConfig:
    def test_static_requires_tree(self):
        with pytest.raises(ValueError):
            HierarchyProviderConfig(kind="static", hierarchy_id="x")

    def test_dimension_backed_requires_dimension_id(self):
        with pytest.raises(ValueError):
            HierarchyProviderConfig(kind="dimension-backed", hierarchy_id="x")

    def test_data_derived_requires_rule(self):
        with pytest.raises(ValueError):
            HierarchyProviderConfig(kind="data-derived", hierarchy_id="x")

    def test_bare_rule_coerces_to_data_derived(self):
        from dynastore.modules.stac.stac_config import HierarchyRule
        rule = HierarchyRule(hierarchy_id="admin0", item_code_field="iso")
        cfg = HierarchyProviderConfig.model_validate(rule.model_dump())
        assert cfg.kind == "data-derived"
        assert cfg.rule is not None
        assert cfg.hierarchy_id == "admin0"

    def test_unknown_kind_rejected(self):
        with pytest.raises(ValueError):
            HierarchyProviderConfig(kind="nope", hierarchy_id="x")  # type: ignore[arg-type]
