#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Tests for the generic, DB-free ``tools.typed_store`` package.

Two kinds of coverage:

1. **Invariant** — nothing under ``dynastore.tools.typed_store`` may import
   SQLAlchemy / asyncpg / the ``db_config`` module. This is what keeps
   dynastore runnable without a database.
2. **Behaviour** — ``InMemoryTypedStore`` obeys the ``TypedStore`` contract:
   exact-match wins over subclass, set/get/delete round-trips, list honours
   limit+offset, migrator DAG is applied on read when schema_id drifts.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import ClassVar, Optional

import pytest

from dynastore.tools.typed_store import (
    CatalogScope,
    CollectionScope,
    InMemoryTypedStore,
    PersistentModel,
    PlatformScope,
    TypedModelRegistry,
    TypedStore,
    compute_schema_id,
    migrate,
    migrates,
)
from dynastore.tools.typed_store import migrations as migrations_mod


# ---------------------------------------------------------------------------
# Invariant: Layer 1 has no DB dependencies
# ---------------------------------------------------------------------------

_FORBIDDEN_ROOTS = (
    "sqlalchemy",
    "asyncpg",
    "dynastore.modules.db_config",
)


def _iter_layer1_modules():
    pkg_root = Path(__file__).resolve().parents[3] / "src" / "dynastore" / "tools" / "typed_store"
    return pkg_root.rglob("*.py")


def test_layer1_has_no_db_imports() -> None:
    offenders: list[tuple[str, str]] = []
    for path in _iter_layer1_modules():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            mod_names: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module:
                mod_names.append(node.module)
            elif isinstance(node, ast.Import):
                mod_names.extend(alias.name for alias in node.names)
            for mod in mod_names:
                if any(mod == root or mod.startswith(root + ".") for root in _FORBIDDEN_ROOTS):
                    offenders.append((str(path.relative_to(path.parents[4])), mod))
    assert not offenders, (
        "Layer 1 (tools/typed_store) must stay DB-free. Offenders: " + repr(offenders)
    )


# ---------------------------------------------------------------------------
# Fixtures: isolated registry + migrator DAG per test
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_migrations():
    migrations_mod.clear()
    yield
    migrations_mod.clear()


# ---------------------------------------------------------------------------
# Behaviour
# ---------------------------------------------------------------------------


class _Base(PersistentModel):
    _class_key: ClassVar[Optional[str]] = "test_typed_store._Base"
    x: int = 1


class _Sub(_Base):
    _class_key: ClassVar[Optional[str]] = "test_typed_store._Sub"
    y: str = "hi"


def test_protocol_structural_match() -> None:
    assert isinstance(InMemoryTypedStore(), TypedStore)


async def test_set_get_delete_roundtrip() -> None:
    store = InMemoryTypedStore()
    scope = PlatformScope()

    assert await store.get(scope, _Base) is None
    await store.set(scope, _Base(x=42))
    got = await store.get(scope, _Base)
    assert got is not None and got.x == 42

    await store.delete(scope, _Base)
    assert await store.get(scope, _Base) is None


async def test_exact_match_wins_over_subclass() -> None:
    store = InMemoryTypedStore()
    scope = CatalogScope(catalog_id="c1")

    await store.set(scope, _Sub(x=1, y="from_sub"))
    await store.set(scope, _Base(x=99))

    # Asking for _Base should return the exact _Base row, not the _Sub.
    got = await store.get(scope, _Base)
    assert type(got) is _Base
    assert got is not None and got.x == 99

    # Asking for _Sub returns the _Sub row.
    got_sub = await store.get(scope, _Sub)
    assert type(got_sub) is _Sub
    assert got_sub is not None and got_sub.y == "from_sub"


async def test_subclass_returned_when_no_exact_match() -> None:
    store = InMemoryTypedStore()
    scope = CollectionScope(catalog_id="c1", collection_id="coll")

    await store.set(scope, _Sub(x=7, y="only_sub"))
    got = await store.get(scope, _Base)
    assert type(got) is _Sub  # covariant read: runtime is the specialised subclass
    assert got is not None and got.x == 7


async def test_scopes_are_isolated() -> None:
    store = InMemoryTypedStore()
    await store.set(PlatformScope(), _Base(x=1))
    await store.set(CatalogScope(catalog_id="c1"), _Base(x=2))

    a = await store.get(PlatformScope(), _Base)
    b = await store.get(CatalogScope(catalog_id="c1"), _Base)
    c = await store.get(CatalogScope(catalog_id="c2"), _Base)
    assert a is not None and a.x == 1
    assert b is not None and b.x == 2
    assert c is None


async def test_list_limit_offset_newest_first() -> None:
    store = InMemoryTypedStore()
    scope = PlatformScope()
    # _Sub and _Base both match when listing under _Base.
    await store.set(scope, _Base(x=1))
    await store.set(scope, _Sub(x=2, y="a"))

    rows = await store.list(scope, _Base, limit=10, offset=0)
    assert len(rows) == 2
    # newest-first: the _Sub was set last
    assert type(rows[0]) is _Sub

    one = await store.list(scope, _Base, limit=1, offset=1)
    assert len(one) == 1
    assert type(one[0]) is _Base


# ---------------------------------------------------------------------------
# Migrator DAG
# ---------------------------------------------------------------------------


def test_compute_schema_id_is_deterministic() -> None:
    class _M(PersistentModel):
        _class_key: ClassVar[Optional[str]] = "test_typed_store._M"
        a: int = 1

    assert compute_schema_id(_M) == _M.schema_id()
    assert _M.schema_id().startswith("sha256:")


def test_migrator_find_path_and_apply(clean_migrations) -> None:
    h1, h2, h3 = "sha256:aaa", "sha256:bbb", "sha256:ccc"

    @migrates(source=h1, target=h2)
    def _a(d):
        d["step1"] = True
        return d

    @migrates(source=h2, target=h3)
    def _b(d):
        d["step2"] = True
        return d

    out = migrate({}, source=h1, target=h3)
    assert out == {"step1": True, "step2": True}


def test_migrator_missing_path_raises(clean_migrations) -> None:
    with pytest.raises(LookupError):
        migrate({}, source="sha256:x", target="sha256:y")


def test_duplicate_migrator_rejected(clean_migrations) -> None:
    h1, h2 = "sha256:a", "sha256:b"

    @migrates(source=h1, target=h2)
    def _first(d):
        return d

    with pytest.raises(ValueError):
        @migrates(source=h1, target=h2)
        def _second(d):
            return d


# ---------------------------------------------------------------------------
# Registry duplicate detection
# ---------------------------------------------------------------------------


def test_duplicate_class_key_rejected() -> None:
    key = "test_typed_store._DupeKey"

    class _First(PersistentModel):
        _class_key: ClassVar[Optional[str]] = key

    with pytest.raises(ValueError):
        class _Second(PersistentModel):  # noqa: F841 — registration side-effect
            _class_key: ClassVar[Optional[str]] = key

    # Clean up so later tests aren't polluted.
    TypedModelRegistry._by_key.pop(key, None)  # type: ignore[attr-defined]
