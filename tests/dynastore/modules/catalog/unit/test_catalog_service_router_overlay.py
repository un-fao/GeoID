"""Unit tests for the M2.4 catalog read-flip (router overlay).

Covers:

- ``_unpack_catalog_row`` with ``router_metadata`` overlays router keys
  on top of the legacy SELECT row.
- When ``router_metadata`` is ``None``, behaviour is identical to the
  pre-M2.4 unpack (no regression for callers that don't wire the router).
- ``conforms_to`` in the router payload is also mirrored to
  ``conformsTo`` so Pydantic's alias resolves after validation.
- ``_resolve_catalog_router_metadata`` degrades to ``None`` on router
  exceptions (router outage cannot break catalog reads).

These are isolated from a live DB — all DQL / router calls are patched
or constructed with in-memory dicts.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_service_instance():
    """Build a minimal CatalogService instance without init.__init__ side effects.

    Using ``__new__`` avoids running the real ``__init__`` which wires
    protocols and would require a full test harness.  The unpack /
    router-resolve helpers don't depend on ``self.engine`` in the code
    paths under test, so the bare object is sufficient.
    """
    from dynastore.modules.catalog.catalog_service import CatalogService

    return CatalogService.__new__(CatalogService)


# ---------------------------------------------------------------------------
# _unpack_catalog_row + router overlay
# ---------------------------------------------------------------------------


class _FakeRow:
    """Row object mimicking SQLAlchemy's ``_mapping`` shape."""

    def __init__(self, mapping):
        self._mapping = dict(mapping)


def test_unpack_without_router_metadata_preserves_legacy_behaviour():
    """``router_metadata=None`` path returns exactly what the legacy code did.

    Guards against regressions for any call site that hasn't been
    migrated to the router-aware overlay.
    """
    svc = _make_service_instance()
    row = _FakeRow({
        "id": "cat", "type": "Catalog", "physical_schema": "t_alpha",
        "title": {"en": "Legacy title"}, "description": {"en": "D"},
        "keywords": {"en": ["k"]}, "license": "CC-BY-4.0",
        "conforms_to": ["https://…/core"],
        "links": [], "assets": {}, "extra_metadata": {},
        "stac_version": "1.1.0", "stac_extensions": [],
        "provisioning_status": "ready",
    })
    cat = svc._unpack_catalog_row(row)
    assert cat is not None
    assert cat.id == "cat"
    # LocalizedText serialises all language slots; just pin the 'en' entry.
    assert cat.title.model_dump(exclude_none=True) == {"en": "Legacy title"}


def test_unpack_router_metadata_overrides_legacy_columns():
    """Router-supplied fields take precedence over the legacy SELECT."""
    svc = _make_service_instance()
    row = _FakeRow({
        "id": "cat", "type": "Catalog", "physical_schema": "t_alpha",
        "title": {"en": "Legacy title"},        # legacy column
        "description": {"en": "Legacy desc"},   # legacy column
        "keywords": None, "license": None,
        "conforms_to": None,
        "links": [], "assets": {}, "extra_metadata": {},
        "stac_version": "1.1.0", "stac_extensions": [],
        "provisioning_status": "ready",
    })
    router_metadata = {
        "title": {"en": "Router title"},         # overrides legacy
        "description": {"en": "Router desc"},    # overrides legacy
    }
    cat = svc._unpack_catalog_row(row, router_metadata=router_metadata)
    assert cat is not None
    assert cat.title.model_dump(exclude_none=True) == {"en": "Router title"}
    assert cat.description.model_dump(exclude_none=True) == {"en": "Router desc"}


def test_unpack_router_conforms_to_populates_both_spellings():
    """``conforms_to`` in router payload fills both snake_case and camelCase.

    The catalog.catalogs legacy column is ``conforms_to`` but the
    Catalog model exposes ``conformsTo`` via Pydantic alias.  The
    router returns ``conforms_to`` (snake_case column name); the
    unpack must populate ``conformsTo`` so the Pydantic validator
    sees it, regardless of which alias the downstream dump uses.
    """
    svc = _make_service_instance()
    row = _FakeRow({
        "id": "cat", "type": "Catalog", "physical_schema": "t_alpha",
        "title": None, "description": None,
        "conforms_to": None,
        "links": [], "assets": {}, "extra_metadata": {},
        "stac_version": "1.1.0", "stac_extensions": [],
        "provisioning_status": "ready",
    })
    router_metadata = {"conforms_to": ["https://…/core", "https://…/oaf"]}
    cat = svc._unpack_catalog_row(row, router_metadata=router_metadata)
    assert cat is not None
    # After validation, the alias-resolved attribute surfaces the list.
    assert cat.conformsTo == ["https://…/core", "https://…/oaf"]


def test_unpack_returns_none_for_empty_row_regardless_of_router():
    """No row → None, even if the router had data (catalog doesn't exist)."""
    svc = _make_service_instance()
    assert svc._unpack_catalog_row(None, router_metadata={"title": "T"}) is None


# ---------------------------------------------------------------------------
# _resolve_catalog_router_metadata — degrade-on-error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_router_resolution_returns_router_payload_on_success(monkeypatch):
    svc = _make_service_instance()

    async def _fake_get(catalog_id, *, context=None, db_resource=None):
        return {"title": {"en": "T"}, "conforms_to": ["x"]}

    monkeypatch.setattr(
        "dynastore.modules.catalog.catalog_metadata_router.get_catalog_metadata",
        _fake_get,
    )
    result = await svc._resolve_catalog_router_metadata("cat-42", db_resource=None)
    assert result == {"title": {"en": "T"}, "conforms_to": ["x"]}


@pytest.mark.asyncio
async def test_router_resolution_degrades_to_none_on_exception(monkeypatch, caplog):
    """Router raising MUST NOT 5xx the catalog read — fall back to legacy SELECT."""
    svc = _make_service_instance()

    async def _boom(catalog_id, *, context=None, db_resource=None):
        raise RuntimeError("router exploded")

    monkeypatch.setattr(
        "dynastore.modules.catalog.catalog_metadata_router.get_catalog_metadata",
        _boom,
    )
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.catalog.catalog_service",
    ):
        result = await svc._resolve_catalog_router_metadata(
            "cat-42", db_resource=None,
        )
    assert result is None
    assert any(
        "router failed" in r.message.lower() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_router_resolution_returns_none_when_router_returns_none(monkeypatch):
    """No router data → fall back cleanly to legacy columns (no synthesis)."""
    svc = _make_service_instance()

    async def _none(catalog_id, *, context=None, db_resource=None):
        return None

    monkeypatch.setattr(
        "dynastore.modules.catalog.catalog_metadata_router.get_catalog_metadata",
        _none,
    )
    result = await svc._resolve_catalog_router_metadata(
        "cat-42", db_resource=None,
    )
    assert result is None
