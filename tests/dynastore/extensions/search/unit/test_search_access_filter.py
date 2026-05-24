"""Row-level ABAC read-path wire-up through SearchService (#1285).

Proves the gated access-filter dispatch:

* an access-aware SEARCH driver (``applies_access_filter=True``) makes the
  service compile the caller's read scope and AND the translated clause into
  the ES query body;
* a non-access-aware driver leaves the query byte-for-byte unchanged (existing
  drivers/collections are unaffected);
* anonymous callers still get the filter compiled (public-only scope), never
  skipped;
* a missing PermissionProtocol fails closed (``match_none``);
* the ``-envelope-items`` index maps to the canonical envelope field shape.
"""

from __future__ import annotations

import pytest

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import (
    SearchService,
    _index_is_canonical,
)
from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)


def _service() -> SearchService:
    return SearchService.__new__(SearchService)


def _capturing_driver():
    """A fake items driver whose ``es_client.search`` records the ES body."""
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, **kwargs):
            captured["index"] = index
            captured["body"] = body
            captured["kwargs"] = kwargs
            return {"hits": {"hits": [], "total": {"value": 0}}}

    class _FakeDriver:
        es_client = _FakeES()

    return _FakeDriver(), captured


@pytest.fixture(autouse=True)
def _patch_index_prefix(monkeypatch):
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "test",
    )


# ---------------------------------------------------------------------------
# (c) Envelope-index detection maps to the canonical shape.
# ---------------------------------------------------------------------------

def test_envelope_index_is_canonical() -> None:
    assert _index_is_canonical("test-acme-envelope-items") is True
    assert _index_is_canonical("test-acme-private-items") is True
    # Public per-catalog index / platform alias stay non-canonical.
    assert _index_is_canonical("test-acme-items") is False
    assert _index_is_canonical("test-items") is False


async def test_envelope_index_uses_canonical_envelope_fields(monkeypatch):
    """A resolved ``-envelope-items`` index addresses the canonical field
    shape (``collection_id``) — the structural ``collections`` filter must hit
    the canonical name, not the public ``collection``."""
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    # Resolve to the envelope index, but keep the driver non-access-aware here
    # so this test isolates the *field-shape* behaviour from filter injection.
    monkeypatch.setattr(
        svc, "_resolve_items_index",
        _const_coro("test-acme-envelope-items"),
    )

    await svc.search_items(SearchBody(catalog_id="acme", collections=["c"], limit=10))

    body_str = str(captured["body"])
    assert "collection_id" in body_str
    assert "\"collection\"" not in body_str


# ---------------------------------------------------------------------------
# Helpers for filter-injection tests.
# ---------------------------------------------------------------------------

def _const_coro(value):
    async def _coro(*args, **kwargs):
        return value
    return _coro


class _Perms:
    """Minimal PermissionProtocol double recording its compile call."""

    def __init__(self, access_filter):
        self._af = access_filter
        self.calls: list = []

    async def compile_read_filter(
        self, principals, catalog_id=None, collection_id=None, *, principal=None,
    ):
        self.calls.append(
            {
                "principals": principals,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "principal": principal,
            }
        )
        return self._af


def _wire_access_aware(svc, monkeypatch, *, access_aware: bool):
    """Make the resolved SEARCH driver (non-)access-aware for the dispatch."""

    class _Driver:
        applies_access_filter = access_aware

    async def _resolve_instance(catalog_id, collections, driver_hint, *, scoped):
        return _Driver() if scoped else None

    monkeypatch.setattr(svc, "_resolve_search_driver_instance", _resolve_instance)


def _patch_perms(monkeypatch, perms):
    """Patch ``get_protocol`` to return ``perms`` ONLY for PermissionProtocol.

    Everything else (e.g. ConfigsProtocol used by the sort known-fields lookup)
    resolves to ``None`` so those side paths degrade gracefully and the test
    isolates the access-filter behaviour.
    """
    from dynastore.models.protocols.policies import PermissionProtocol

    def _get_protocol(proto, *a, **k):
        return perms if proto is PermissionProtocol else None

    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", _get_protocol)


# ---------------------------------------------------------------------------
# (a) Access-aware driver → filter compiled and ANDed into the query body.
# ---------------------------------------------------------------------------

async def test_access_aware_driver_applies_compiled_filter(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    monkeypatch.setattr(
        svc, "_resolve_items_index", _const_coro("test-acme-envelope-items"),
    )
    _wire_access_aware(svc, monkeypatch, access_aware=True)

    af = AccessFilter.from_clauses(
        allow=[AccessClause((FieldPredicate("owner", ("alice",)),))],
    )
    perms = _Perms(af)
    _patch_perms(monkeypatch, perms)

    await svc.search_items(
        SearchBody(catalog_id="acme", collections=["c"], limit=10),
        scoped=True,
        principals=["oidc:alice", "user"],
        principal={"sub": "alice"},
    )

    # The compiled filter's clause landed in the query body.
    body_str = str(captured["body"])
    assert "owner" in body_str
    assert "alice" in body_str
    # compile_read_filter received the threaded principals + single-collection.
    assert perms.calls and perms.calls[0]["principals"] == ["oidc:alice", "user"]
    assert perms.calls[0]["catalog_id"] == "acme"
    assert perms.calls[0]["collection_id"] == "c"
    assert perms.calls[0]["principal"] == {"sub": "alice"}


async def test_access_aware_anonymous_still_compiles_filter(monkeypatch):
    """Anonymous (principal=None, only an anonymous-role principals list) MUST
    still compile a filter — skipping it would leak."""
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    monkeypatch.setattr(
        svc, "_resolve_items_index", _const_coro("test-acme-envelope-items"),
    )
    _wire_access_aware(svc, monkeypatch, access_aware=True)

    perms = _Perms(AccessFilter.deny_everything())
    _patch_perms(monkeypatch, perms)

    await svc.search_items(
        SearchBody(catalog_id="acme", collections=["c"], limit=10),
        scoped=True,
        principals=["unauthenticated"],
        principal=None,
    )

    assert perms.calls and perms.calls[0]["principals"] == ["unauthenticated"]
    # deny_everything → match_none in the body.
    assert "match_none" in str(captured["body"])


async def test_access_aware_but_no_permission_protocol_fails_closed(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    monkeypatch.setattr(
        svc, "_resolve_items_index", _const_coro("test-acme-envelope-items"),
    )
    _wire_access_aware(svc, monkeypatch, access_aware=True)
    _patch_perms(monkeypatch, None)

    await svc.search_items(
        SearchBody(catalog_id="acme", collections=["c"], limit=10),
        scoped=True,
        principals=["unauthenticated"],
    )

    # Fail closed: with no PermissionProtocol the shared compile helper returns
    # deny-everything, which translates to a match_none clause AND-ed into the
    # query — the result matches nothing (no unfiltered scan), regardless of the
    # exact bool wrapping.
    import json as _json

    assert "match_none" in _json.dumps(captured["body"]["query"])


# ---------------------------------------------------------------------------
# (b) Non-access-aware driver → query untouched (existing drivers unaffected).
# ---------------------------------------------------------------------------

async def test_non_access_aware_driver_leaves_query_unchanged(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    monkeypatch.setattr(
        svc, "_resolve_items_index", _const_coro("test-acme-items"),
    )
    _wire_access_aware(svc, monkeypatch, access_aware=False)

    # A PermissionProtocol whose compile_read_filter would blow up — proving the
    # non-access-aware path never reaches the permission engine (the gate returns
    # before any compile). ``get_protocol`` returns None for everything else so
    # the unrelated config lookups degrade gracefully.
    class _Boom:
        async def compile_read_filter(self, *a, **k):
            raise AssertionError(
                "compile_read_filter must not run for a non-access-aware driver"
            )

    from dynastore.models.protocols.policies import PermissionProtocol

    def _get_protocol(proto, *a, **k):
        return _Boom() if proto is PermissionProtocol else None

    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", _get_protocol)

    await svc.search_items(
        SearchBody(catalog_id="acme", collections=["c"], limit=10),
        scoped=True,
        principals=["oidc:alice"],
    )

    # No access fields anywhere in the body.
    body_str = str(captured["body"])
    assert "owner" not in body_str
    assert "grant_subjects" not in body_str
    assert "match_none" not in body_str


async def test_unscoped_public_search_never_applies_filter(monkeypatch):
    """The unscoped public route resolves no driver instance, so no filter is
    ever compiled even though a catalog_id is supplied."""
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)
    monkeypatch.setattr(
        svc, "_resolve_items_index", _const_coro("test-acme-items"),
    )
    # Real ``_resolve_search_driver_instance``: scoped=False ⟹ returns None ⟹
    # ``getattr(None, "applies_access_filter", False)`` is False ⟹ no compile.

    class _Boom:
        async def compile_read_filter(self, *a, **k):
            raise AssertionError(
                "unscoped search must not reach the permission engine"
            )

    from dynastore.models.protocols.policies import PermissionProtocol

    def _get_protocol(proto, *a, **k):
        return _Boom() if proto is PermissionProtocol else None

    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", _get_protocol)

    await svc.search_items(
        SearchBody(catalog_id="acme", collections=["c"], limit=10),
        scoped=False,
        principals=["oidc:alice"],
    )

    assert "owner" not in str(captured["body"])
