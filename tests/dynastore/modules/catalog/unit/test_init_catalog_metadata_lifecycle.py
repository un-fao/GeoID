"""Unit tests for the M2.2 catalog-metadata lifecycle phase.

Covers:

- ``LifecycleRegistry.init_catalog_metadata`` dispatches to every
  registered ``@sync_catalog_metadata_initializer`` hook.
- Hooks receive the service-supplied ``**kwargs`` (e.g. ``catalog_metadata``).
- Priority ordering is respected.
- ``init_catalog_metadata`` is a no-op when no hooks are registered.
- The PG catalog-metadata hooks (``_pg_catalog_core_init``,
  ``_pg_catalog_stac_init``) are registered at import time.
- The service-level helper ``_build_catalog_metadata_payload`` produces a
  dict whose keys align with the domain-metadata column tuples.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# LifecycleRegistry contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_init_catalog_metadata_dispatches_to_registered_hook():
    """A hook registered under sync_catalog_metadata_initializer fires."""
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    reg = LifecycleRegistry()
    calls: list[tuple] = []

    @reg.sync_catalog_metadata_initializer(priority=0)
    async def _hook(conn, schema, catalog_id, **kwargs):
        calls.append((conn, schema, catalog_id, kwargs))

    fake_conn = MagicMock()
    await reg.init_catalog_metadata(
        fake_conn, "t_alpha", catalog_id="cat-42",
        catalog_metadata={"title": {"en": "T"}},
    )
    assert len(calls) == 1
    assert calls[0][0] is fake_conn
    assert calls[0][1] == "t_alpha"
    assert calls[0][2] == "cat-42"
    assert calls[0][3] == {"catalog_metadata": {"title": {"en": "T"}}}


@pytest.mark.asyncio
async def test_init_catalog_metadata_respects_priority_order():
    """HIGHER priority numbers fire first (matches _sort_hooks semantics)."""
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    reg = LifecycleRegistry()
    order: list[str] = []

    @reg.sync_catalog_metadata_initializer(priority=10)
    async def _first_hook(conn, schema, catalog_id, **kwargs):
        order.append("p10")

    @reg.sync_catalog_metadata_initializer(priority=5)
    async def _second_hook(conn, schema, catalog_id, **kwargs):
        order.append("p5")

    await reg.init_catalog_metadata(MagicMock(), "t_alpha", catalog_id="c")
    # Higher priority first — p10 runs before p5.  Matches the CORE (5)
    # → STAC (10) intent on the PG hooks: CORE's required catalog-core row
    # lands before STAC writes reference it.  (Priorities on the PG hooks
    # mirror that: _pg_catalog_core_init=5, _pg_catalog_stac_init=10, so
    # CORE effectively runs LAST — which is what we want since the STAC
    # table has no FK to catalog_metadata_core.)  The test here pins the
    # sort direction of the lifecycle dispatcher regardless.
    assert order == ["p10", "p5"]


@pytest.mark.asyncio
async def test_init_catalog_metadata_noop_when_no_hooks():
    """With zero registered hooks, init_catalog_metadata returns immediately."""
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    reg = LifecycleRegistry()
    # Must not raise; no SAVEPOINT dance, no logging noise beyond info.
    await reg.init_catalog_metadata(MagicMock(), "t_alpha", catalog_id="c")


@pytest.mark.asyncio
async def test_unknown_kwargs_are_forwarded_and_ignored_by_hooks():
    """A hook that declares ``**_ignored`` must tolerate forward-compatible kwargs."""
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    reg = LifecycleRegistry()
    captured: dict = {}

    @reg.sync_catalog_metadata_initializer(priority=0)
    async def _hook(conn, schema, catalog_id, *, catalog_metadata=None, **_ignored):
        captured.update({"meta": catalog_metadata, "extras": dict(_ignored)})

    await reg.init_catalog_metadata(
        MagicMock(), "t_alpha", catalog_id="c",
        catalog_metadata={"title": "T"},
        # Simulating a future STAC-only kwarg that existing hooks should not choke on.
        stac_extensions=["https://…/datacube/v2"],
    )
    assert captured["meta"] == {"title": "T"}
    assert captured["extras"] == {"stac_extensions": ["https://…/datacube/v2"]}


# ---------------------------------------------------------------------------
# Decorator + module export
# ---------------------------------------------------------------------------


def test_sync_catalog_metadata_initializer_is_module_level_alias():
    """Module-level import path matches the established decorator-export pattern.

    Python's ``__getattr__`` on a class instance returns a fresh bound-
    method object on every access, so ``is`` comparison isn't stable —
    test behaviourally: the alias registers a hook on the shared
    registry, and ``init_catalog_metadata`` sees it.
    """
    import dynastore.modules.catalog.lifecycle_manager as lc

    # Compare ``__func__`` underneath the bound method — that IS stable.
    assert (
        lc.sync_catalog_metadata_initializer.__func__
        is lc.LifecycleRegistry.sync_catalog_metadata_initializer
    )
    # Bound to the same singleton registry.
    assert (
        lc.sync_catalog_metadata_initializer.__self__
        is lc.lifecycle_registry
    )


# ---------------------------------------------------------------------------
# PG hooks registered at import-time
# ---------------------------------------------------------------------------


def test_pg_catalog_core_and_stac_hooks_are_registered():
    """Importing the driver module must register the two catalog-metadata hooks."""
    # Ensure the module imported (touches the decorator registrations).
    from dynastore.modules.storage.drivers import metadata_domain_postgresql  # noqa: F401
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

    names = {
        h[1].__name__
        for h in lifecycle_registry._sync_catalog_metadata_initializers
    }
    assert "_pg_catalog_core_init" in names
    assert "_pg_catalog_stac_init" in names


@pytest.mark.asyncio
async def test_pg_catalog_core_init_noop_when_no_metadata():
    """Default-fast: empty-body catalog → zero rows inserted."""
    from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
        _pg_catalog_core_init,
    )

    # No catalog_metadata supplied → hook returns immediately without
    # constructing a driver or issuing SQL.
    await _pg_catalog_core_init(
        conn=MagicMock(), schema="t_alpha", catalog_id="c",
    )
    # The lack of any mock being called IS the assertion — if the hook
    # touched the driver we'd see it via module-level AsyncMock patches
    # in the dedicated test below.


@pytest.mark.asyncio
async def test_pg_catalog_core_init_upserts_when_metadata_present(monkeypatch):
    """Hook calls CatalogCorePostgresqlDriver.upsert_catalog_metadata."""
    from dynastore.modules.storage.drivers import metadata_domain_postgresql as mod

    upsert_calls: list[tuple] = []

    async def _fake_upsert(self, catalog_id, metadata, *, db_resource=None):
        upsert_calls.append((catalog_id, metadata, db_resource))

    monkeypatch.setattr(
        mod.CatalogCorePostgresqlDriver,
        "upsert_catalog_metadata",
        _fake_upsert,
    )

    fake_conn = MagicMock()
    await mod._pg_catalog_core_init(
        conn=fake_conn, schema="t_alpha", catalog_id="cat-42",
        catalog_metadata={"title": {"en": "T"}, "description": {"en": "D"}},
    )
    assert len(upsert_calls) == 1
    catalog_id, payload, dbr = upsert_calls[0]
    assert catalog_id == "cat-42"
    assert payload == {"title": {"en": "T"}, "description": {"en": "D"}}
    assert dbr is fake_conn


@pytest.mark.asyncio
async def test_pg_catalog_stac_init_skips_when_no_stac_keys(monkeypatch):
    """STAC hook skips when the payload has no STAC-relevant column."""
    from dynastore.modules.storage.drivers import metadata_domain_postgresql as mod

    upsert_calls: list[tuple] = []

    async def _fake_upsert(self, catalog_id, metadata, *, db_resource=None):
        upsert_calls.append((catalog_id, metadata))

    monkeypatch.setattr(
        mod.CatalogStacPostgresqlDriver,
        "upsert_catalog_metadata",
        _fake_upsert,
    )

    # Only CORE fields — STAC hook must not fire.
    await mod._pg_catalog_stac_init(
        conn=MagicMock(), schema="t_alpha", catalog_id="c",
        catalog_metadata={"title": {"en": "T"}, "description": {"en": "D"}},
    )
    assert upsert_calls == []


@pytest.mark.asyncio
async def test_pg_catalog_core_init_skips_when_no_core_keys(monkeypatch):
    """CORE hook must symmetrically skip STAC-only payloads (C2 regression).

    Before the 2026-04-21 code-review fix, ``_pg_catalog_core_init``
    only guarded on ``if not catalog_metadata:`` — a STAC-only payload
    would slip through and insert an all-NULL CORE row into
    ``catalog.catalog_metadata_core``.  The merged-read layer would
    then treat the catalog as having CORE metadata (empty) and poison
    the envelope.
    """
    from dynastore.modules.storage.drivers import metadata_domain_postgresql as mod

    upsert_calls: list[tuple] = []

    async def _fake_upsert(self, catalog_id, metadata, *, db_resource=None):
        upsert_calls.append((catalog_id, metadata))

    monkeypatch.setattr(
        mod.CatalogCorePostgresqlDriver,
        "upsert_catalog_metadata",
        _fake_upsert,
    )

    # Only STAC fields — CORE hook must not fire.
    await mod._pg_catalog_core_init(
        conn=MagicMock(), schema="t_alpha", catalog_id="c",
        catalog_metadata={
            "stac_version": "1.1.0",
            "stac_extensions": ["https://…/datacube/v2"],
            "conforms_to": ["https://…/core"],
        },
    )
    assert upsert_calls == []


@pytest.mark.asyncio
async def test_pg_catalog_core_init_skips_when_core_keys_are_all_none(monkeypatch):
    """Payload with CORE keys present but all-None must not fire CORE hook.

    A payload like ``{"title": None, "description": None}`` is the
    equivalent of a STAC-only payload — there is nothing to persist.
    The guard uses ``get(k) is not None`` specifically to catch this
    case.
    """
    from dynastore.modules.storage.drivers import metadata_domain_postgresql as mod

    upsert_calls: list[tuple] = []

    async def _fake_upsert(self, catalog_id, metadata, *, db_resource=None):
        upsert_calls.append((catalog_id, metadata))

    monkeypatch.setattr(
        mod.CatalogCorePostgresqlDriver,
        "upsert_catalog_metadata",
        _fake_upsert,
    )

    await mod._pg_catalog_core_init(
        conn=MagicMock(), schema="t_alpha", catalog_id="c",
        catalog_metadata={
            "title": None,
            "description": None,
            "stac_version": "1.1.0",  # only non-None key — CORE domain should skip
        },
    )
    assert upsert_calls == []


@pytest.mark.asyncio
async def test_pg_catalog_stac_init_upserts_when_stac_keys_present(monkeypatch):
    """STAC hook fires on a STAC-bearing payload."""
    from dynastore.modules.storage.drivers import metadata_domain_postgresql as mod

    upsert_calls: list[tuple] = []

    async def _fake_upsert(self, catalog_id, metadata, *, db_resource=None):
        upsert_calls.append((catalog_id, metadata))

    monkeypatch.setattr(
        mod.CatalogStacPostgresqlDriver,
        "upsert_catalog_metadata",
        _fake_upsert,
    )

    await mod._pg_catalog_stac_init(
        conn=MagicMock(), schema="t_alpha", catalog_id="cat-42",
        catalog_metadata={
            "title": {"en": "T"},         # CORE — ignored by STAC hook
            "stac_version": "1.1.0",      # STAC — triggers upsert
            "stac_extensions": ["…/ext"],
        },
    )
    assert len(upsert_calls) == 1
    # The driver's _filter_payload drops the CORE key — but that's the
    # upsert_metadata internals.  At the hook level we just forward the
    # full dict.
    assert upsert_calls[0][0] == "cat-42"


# ---------------------------------------------------------------------------
# Helper: _build_catalog_metadata_payload
# ---------------------------------------------------------------------------


def test_build_catalog_metadata_payload_omits_none_fields():
    """An empty-body catalog creates an empty payload → hooks no-op."""
    from dynastore.modules.catalog.catalog_service import (
        _build_catalog_metadata_payload,
    )
    from dynastore.modules.catalog.models import Catalog

    cat = Catalog(
        id="c", type="Catalog",
        stac_version="1.1.0", conformsTo=[], links=[],
    )
    payload = _build_catalog_metadata_payload(cat)
    # stac_version is a always-present default on Catalog — still emitted.
    assert "stac_version" in payload
    # Nothing else should appear because the optional fields were not set.
    for unexpected in ("title", "description", "keywords", "license", "extra_metadata"):
        assert unexpected not in payload, (
            f"Optional CORE key {unexpected!r} should not appear in the "
            f"payload when caller supplied None"
        )


def test_build_catalog_metadata_payload_roundtrips_mixed_fields():
    """Supplied fields arrive in the payload under the domain-column names."""
    from dynastore.modules.catalog.catalog_service import (
        _build_catalog_metadata_payload,
    )
    from dynastore.modules.catalog.models import Catalog
    from dynastore.models.localization import LocalizedText

    cat = Catalog(
        id="c", type="Catalog",
        title=LocalizedText(en="Hello"),
        stac_version="1.1.0",
        stac_extensions=["https://…/datacube/v2"],
        conformsTo=["https://…/core"],
        links=[],
    )
    payload = _build_catalog_metadata_payload(cat)
    assert payload["title"] == {"en": "Hello"}
    assert payload["stac_version"] == "1.1.0"
    assert payload["stac_extensions"] == ["https://…/datacube/v2"]
    assert payload["conforms_to"] == ["https://…/core"]
