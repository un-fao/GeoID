"""Unit tests for ``LookupOnlySearchHandler``.

Verifies the handler used to gate /search and /search/catalogs/{cat} as
a needle-lookup-only surface — the request must carry a lookup field
(``geoid`` or ``external_id``) and must not carry a broadening field
(``bbox`` / ``intersects`` / ``datetime`` / ``filter`` / ``q``).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pytest

from dynastore.modules.iam.conditions import (
    EvaluationContext,
    LookupOnlySearchHandler,
)


pytestmark = pytest.mark.asyncio


class _StubRequest:
    """Minimal Starlette-Request-shaped stub.

    Only ``json()`` is exercised by the handler on POST. The handler
    accepts any object whose ``json()`` coroutine returns a dict, or
    raises (which is treated as fail-closed).
    """

    def __init__(self, body: Any = None, *, raises: Optional[Exception] = None):
        self._body = body
        self._raises = raises

    async def json(self) -> Any:
        if self._raises is not None:
            raise self._raises
        return self._body


def _ctx(
    *,
    method: str = "POST",
    path: str = "/search",
    query: Optional[Dict[str, str]] = None,
    request: Optional[_StubRequest] = None,
) -> EvaluationContext:
    return EvaluationContext(
        request=request,
        storage=None,  # type: ignore[arg-type]
        path=path,
        method=method,
        query_params=query,
    )


@pytest.fixture
def handler() -> LookupOnlySearchHandler:
    return LookupOnlySearchHandler()


# ---------------------------------------------------------------------------
# GET surface — fields come from ctx.query_params
# ---------------------------------------------------------------------------


async def test_get_with_geoid_only_allows(handler):
    ctx = _ctx(method="GET", query={"geoid": "X"})
    assert await handler.evaluate({}, ctx) is True


async def test_get_with_external_id_only_allows(handler):
    ctx = _ctx(method="GET", query={"external_id": "Y"})
    assert await handler.evaluate({}, ctx) is True


async def test_get_with_geoid_plus_bbox_denies(handler):
    ctx = _ctx(method="GET", query={"geoid": "X", "bbox": "1,2,3,4"})
    assert await handler.evaluate({}, ctx) is False


async def test_get_with_no_filter_denies(handler):
    ctx = _ctx(method="GET", query={})
    assert await handler.evaluate({}, ctx) is False


async def test_get_with_only_broadening_denies(handler):
    ctx = _ctx(method="GET", query={"bbox": "1,2,3,4"})
    assert await handler.evaluate({}, ctx) is False


async def test_get_with_geoid_plus_pagination_allows(handler):
    ctx = _ctx(method="GET", query={"geoid": "X", "limit": "10", "page": "2"})
    assert await handler.evaluate({}, ctx) is True


# ---------------------------------------------------------------------------
# POST surface — fields come from ctx.request.json()
# ---------------------------------------------------------------------------


async def test_post_with_geoid_only_allows(handler):
    ctx = _ctx(method="POST", request=_StubRequest({"geoid": ["X"]}))
    assert await handler.evaluate({}, ctx) is True


async def test_post_with_external_id_only_allows(handler):
    ctx = _ctx(method="POST", request=_StubRequest({"external_id": ["Y"]}))
    assert await handler.evaluate({}, ctx) is True


async def test_post_with_geoid_plus_bbox_denies(handler):
    ctx = _ctx(
        method="POST",
        request=_StubRequest({"geoid": ["X"], "bbox": [1.0, 2.0, 3.0, 4.0]}),
    )
    assert await handler.evaluate({}, ctx) is False


async def test_post_with_geoid_plus_filter_denies(handler):
    ctx = _ctx(
        method="POST",
        request=_StubRequest({"geoid": ["X"], "filter": "id = 'a'"}),
    )
    assert await handler.evaluate({}, ctx) is False


async def test_post_with_only_broadening_denies(handler):
    ctx = _ctx(method="POST", request=_StubRequest({"bbox": [1, 2, 3, 4]}))
    assert await handler.evaluate({}, ctx) is False


async def test_post_with_empty_body_denies(handler):
    ctx = _ctx(method="POST", request=_StubRequest({}))
    assert await handler.evaluate({}, ctx) is False


async def test_post_unreadable_body_denies(handler):
    """Fail closed — broken or missing JSON body cannot be needle lookup."""
    ctx = _ctx(method="POST", request=_StubRequest(raises=ValueError("not json")))
    assert await handler.evaluate({}, ctx) is False


async def test_post_non_dict_body_denies(handler):
    """Lists / strings / numbers as body root are not valid lookup shapes."""
    ctx = _ctx(method="POST", request=_StubRequest(["not", "a", "dict"]))
    assert await handler.evaluate({}, ctx) is False


async def test_post_missing_request_denies(handler):
    ctx = _ctx(method="POST", request=None)
    assert await handler.evaluate({}, ctx) is False


async def test_post_with_geoid_plus_pagination_allows(handler):
    ctx = _ctx(
        method="POST",
        request=_StubRequest({"geoid": ["X"], "limit": 10, "page": 2}),
    )
    assert await handler.evaluate({}, ctx) is True


async def test_post_with_geoid_plus_collections_scope_allows(handler):
    """``collections`` narrows scope but does not broaden — should be allowed."""
    ctx = _ctx(
        method="POST",
        request=_StubRequest({"geoid": ["X"], "collections": ["coll1", "coll2"]}),
    )
    assert await handler.evaluate({}, ctx) is True


# ---------------------------------------------------------------------------
# Method gating
# ---------------------------------------------------------------------------


async def test_method_other_than_get_post_denies(handler):
    """PUT / DELETE / PATCH are not part of the search surface."""
    for m in ("PUT", "DELETE", "PATCH"):
        ctx = _ctx(method=m, request=_StubRequest({"geoid": ["X"]}))
        assert await handler.evaluate({}, ctx) is False, f"method={m} should deny"


# ---------------------------------------------------------------------------
# Config overrides
# ---------------------------------------------------------------------------


async def test_custom_lookup_fields_override(handler):
    """Operator can tighten lookup_fields — only ``geoid`` counts here."""
    config = {"lookup_fields": ["geoid"]}
    ctx_pass = _ctx(method="POST", request=_StubRequest({"geoid": ["X"]}))
    assert await handler.evaluate(config, ctx_pass) is True

    ctx_fail = _ctx(method="POST", request=_StubRequest({"external_id": ["Y"]}))
    assert await handler.evaluate(config, ctx_fail) is False


async def test_custom_broadening_fields_override(handler):
    """Operator can broaden the deny-list — adding `ids` rejects id-list search."""
    config = {"broadening_fields": ["bbox", "intersects", "datetime", "filter", "q", "ids"]}
    ctx = _ctx(
        method="POST",
        request=_StubRequest({"geoid": ["X"], "ids": ["item-1"]}),
    )
    assert await handler.evaluate(config, ctx) is False


# ---------------------------------------------------------------------------
# Empty / falsy field semantics — empty string / empty list count as absent
# ---------------------------------------------------------------------------


async def test_empty_geoid_string_treated_as_absent(handler):
    """``geoid=""`` should NOT count as a lookup — query-string artifact."""
    ctx = _ctx(method="GET", query={"geoid": ""})
    assert await handler.evaluate({}, ctx) is False


async def test_empty_geoid_list_treated_as_absent(handler):
    """``{"geoid": []}`` is the absent case — not a lookup."""
    ctx = _ctx(method="POST", request=_StubRequest({"geoid": []}))
    assert await handler.evaluate({}, ctx) is False


async def test_empty_bbox_does_not_broaden(handler):
    """``{"bbox": []}`` should not deny — empty list = absent."""
    ctx = _ctx(method="POST", request=_StubRequest({"geoid": ["X"], "bbox": []}))
    assert await handler.evaluate({}, ctx) is True


# ---------------------------------------------------------------------------
# Registry wiring — the handler is registered under its `type` string
# ---------------------------------------------------------------------------


async def test_handler_is_registered_in_default_registry():
    from dynastore.modules.iam.conditions import condition_registry

    h = condition_registry._handlers.get("lookup_only_search")
    assert isinstance(h, LookupOnlySearchHandler)
