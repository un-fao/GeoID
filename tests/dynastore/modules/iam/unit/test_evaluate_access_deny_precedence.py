"""Regression pin (un-fao/GeoID#731): ``PolicyService.evaluate_access``
applies deny-precedence — the same semantic as
:meth:`evaluate_policy_statements`.

Before #731 the role-based path was first-match-wins over an unordered
policy set, so an overlapping ALLOW+DENY would flip outcome between pod
restarts / DB reseeds. The deny-precedence rewrite makes any matching
DENY beat any matching ALLOW regardless of iteration order, and the
divergence between the two evaluators is closed.
"""

from __future__ import annotations

from typing import Any, List

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.policies import PolicyService


def _service() -> PolicyService:
    """Build a PolicyService with no DB / role-storage dependencies.

    ``evaluate_access`` only needs the iteration logic exercised here —
    the role-lookup branch is skipped by passing ``principals=[]`` and
    feeding policies via ``custom_policies``. ``catalog_id=None`` short-
    circuits ``_resolve_schema`` to the global ``iam`` schema without
    hitting :class:`CatalogsProtocol`.
    """
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]
    return svc


def _allow(pid: str, *, path: str = ".*", method: str = ".*") -> Policy:
    return Policy(id=pid, effect="ALLOW", actions=[method], resources=[path])


def _deny(pid: str, *, path: str = ".*", method: str = ".*") -> Policy:
    return Policy(id=pid, effect="DENY", actions=[method], resources=[path])


async def _call(svc: PolicyService, policies: List[Policy]) -> tuple[bool, str]:
    return await svc.evaluate_access(
        principals=[],
        path="/catalogs/x/items",
        method="GET",
        catalog_id=None,
        custom_policies=policies,
    )


@pytest.mark.asyncio
async def test_deny_wins_when_listed_after_allow() -> None:
    """ALLOW first, DENY second — DENY still wins."""
    allowed, reason = await _call(_service(), [_allow("a1"), _deny("d1")])
    assert allowed is False
    assert "DENY by policy d1" in reason


@pytest.mark.asyncio
async def test_deny_wins_when_listed_before_allow() -> None:
    """DENY first, ALLOW second — DENY still wins. The pre-#731 first-
    match-wins loop would have stopped on the DENY here, which by
    coincidence is the same outcome; the regression value lies in the
    *companion* test above, which the old code would have failed."""
    allowed, reason = await _call(_service(), [_deny("d1"), _allow("a1")])
    assert allowed is False
    assert "DENY by policy d1" in reason


@pytest.mark.asyncio
async def test_bare_allow_returns_allow() -> None:
    allowed, reason = await _call(_service(), [_allow("a1")])
    assert allowed is True
    assert "Allowed by policy a1" in reason


@pytest.mark.asyncio
async def test_bare_deny_returns_deny() -> None:
    allowed, reason = await _call(_service(), [_deny("d1")])
    assert allowed is False
    assert "DENY by policy d1" in reason


@pytest.mark.asyncio
async def test_no_match_defaults_to_deny() -> None:
    """Implicit deny — neither effect matched the request."""
    non_matching = _allow("a1", path="/different/.*")
    allowed, reason = await _call(_service(), [non_matching])
    assert allowed is False
    assert "Deny by Default" in reason


@pytest.mark.asyncio
async def test_first_matching_deny_id_is_surfaced() -> None:
    """When multiple DENYs match, the first one's id appears in the
    reason — operators get a stable diagnostic surface even though the
    deny-wins outcome doesn't depend on order."""
    allowed, reason = await _call(
        _service(), [_deny("d_first"), _allow("a1"), _deny("d_second")]
    )
    assert allowed is False
    assert "d_first" in reason
    assert "d_second" not in reason


@pytest.mark.asyncio
async def test_deny_logs_mention_shadowed_allow(caplog: Any) -> None:
    """Operator visibility: when a DENY shadows an ALLOW the log line
    should call that out so the override is debuggable."""
    import logging

    caplog.set_level(logging.INFO, logger="dynastore.modules.iam.policies")
    await _call(_service(), [_allow("a1"), _deny("d1")])
    msgs = [r.getMessage() for r in caplog.records]
    assert any("deny-precedence" in m and "a1" in m and "d1" in m for m in msgs), msgs
