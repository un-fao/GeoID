"""Unit tests for effect-aware invalid-regex resolution (#1775).

An invalid regex in a condition must never raise and must resolve in a
way that preserves the intended policy effect:

* DENY policy  → invalid regex keeps denying (condition returns True /
                  matched) so the DENY is not silently dropped.
* ALLOW policy → invalid regex does not grant access (condition returns
                  False / non-match) — same behaviour as #1772.
* effect=None  → falls back to non-match (False) — safe default.

The path-gate helper (_path_method_matches / RateLimitHandler) keeps its
fail-open semantics regardless of effect: an invalid path_pattern skips
the rate-limit condition (quota not enforced), which is the correct
availability trade-off for a quota, not an access-control decision.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from dynastore.modules.iam.conditions import (
    AttributeMatchHandler,
    EvaluationContext,
    QueryParamHandler,
    RateLimitHandler,
)

pytestmark = pytest.mark.asyncio

INVALID_PATTERN = "(*("  # re.error on compile


def _ctx(*, effect: str | None = None, query: dict | None = None, path: str = "/x", method: str = "GET") -> EvaluationContext:
    return EvaluationContext(
        request=None,
        storage=MagicMock(),
        path=path,
        method=method,
        query_params=query or {},
        effect=effect,
    )


# ---------------------------------------------------------------------------
# QueryParamHandler — query_match condition
# ---------------------------------------------------------------------------


@pytest.fixture
def qp_handler() -> QueryParamHandler:
    return QueryParamHandler()


async def test_query_match_invalid_regex_allow_effect_is_false(qp_handler):
    """ALLOW policy with invalid regex must NOT grant (returns False)."""
    config = {"param": "q", "pattern": INVALID_PATTERN}
    ctx = _ctx(effect="ALLOW", query={"q": "anything"})
    assert await qp_handler.evaluate(config, ctx) is False


async def test_query_match_invalid_regex_deny_effect_is_true(qp_handler):
    """DENY policy with invalid regex must keep denying (returns True = matched)."""
    config = {"param": "q", "pattern": INVALID_PATTERN}
    ctx = _ctx(effect="DENY", query={"q": "anything"})
    assert await qp_handler.evaluate(config, ctx) is True


async def test_query_match_invalid_regex_none_effect_is_false(qp_handler):
    """Unknown/None effect with invalid regex defaults to non-match (False)."""
    config = {"param": "q", "pattern": INVALID_PATTERN}
    ctx = _ctx(effect=None, query={"q": "anything"})
    assert await qp_handler.evaluate(config, ctx) is False


async def test_query_match_valid_regex_allow_match(qp_handler):
    """Valid pattern that matches: returns True regardless of effect."""
    config = {"param": "q", "pattern": r"hello"}
    ctx = _ctx(effect="ALLOW", query={"q": "hello"})
    assert await qp_handler.evaluate(config, ctx) is True


async def test_query_match_valid_regex_allow_no_match(qp_handler):
    """Valid pattern that does not match: returns False for ALLOW."""
    config = {"param": "q", "pattern": r"hello"}
    ctx = _ctx(effect="ALLOW", query={"q": "world"})
    assert await qp_handler.evaluate(config, ctx) is False


async def test_query_match_valid_regex_deny_match(qp_handler):
    """Valid pattern that matches: returns True for DENY too (no regression)."""
    config = {"param": "q", "pattern": r"hello"}
    ctx = _ctx(effect="DENY", query={"q": "hello"})
    assert await qp_handler.evaluate(config, ctx) is True


async def test_query_match_valid_regex_deny_no_match(qp_handler):
    """Valid pattern that does not match: DENY returns False (condition not met)."""
    config = {"param": "q", "pattern": r"hello"}
    ctx = _ctx(effect="DENY", query={"q": "world"})
    assert await qp_handler.evaluate(config, ctx) is False


# ---------------------------------------------------------------------------
# AttributeMatchHandler — match/regex operator
# ---------------------------------------------------------------------------


@pytest.fixture
def attr_handler() -> AttributeMatchHandler:
    return AttributeMatchHandler()


async def test_attribute_regex_invalid_allow_effect_is_false(attr_handler):
    """ALLOW policy with invalid attribute regex must NOT grant."""
    config = {"attribute": "path", "operator": "regex", "value": INVALID_PATTERN}
    ctx = _ctx(effect="ALLOW", path="/any/path")
    assert await attr_handler.evaluate(config, ctx) is False


async def test_attribute_regex_invalid_deny_effect_is_true(attr_handler):
    """DENY policy with invalid attribute regex must keep denying."""
    config = {"attribute": "path", "operator": "regex", "value": INVALID_PATTERN}
    ctx = _ctx(effect="DENY", path="/any/path")
    assert await attr_handler.evaluate(config, ctx) is True


async def test_attribute_regex_invalid_none_effect_is_false(attr_handler):
    """None effect with invalid attribute regex defaults to non-match."""
    config = {"attribute": "path", "operator": "regex", "value": INVALID_PATTERN}
    ctx = _ctx(effect=None, path="/any/path")
    assert await attr_handler.evaluate(config, ctx) is False


async def test_attribute_regex_valid_match_allow(attr_handler):
    """Valid pattern that matches: ALLOW returns True (no regression)."""
    config = {"attribute": "path", "operator": "regex", "value": r"/items"}
    ctx = _ctx(effect="ALLOW", path="/items")
    assert await attr_handler.evaluate(config, ctx) is True


async def test_attribute_regex_valid_no_match_allow(attr_handler):
    """Valid pattern that does not match: ALLOW returns False (no regression)."""
    config = {"attribute": "path", "operator": "regex", "value": r"/items"}
    ctx = _ctx(effect="ALLOW", path="/other")
    assert await attr_handler.evaluate(config, ctx) is False


async def test_attribute_regex_valid_match_deny(attr_handler):
    """Valid pattern that matches: DENY returns True (no regression)."""
    config = {"attribute": "path", "operator": "regex", "value": r"/items"}
    ctx = _ctx(effect="DENY", path="/items")
    assert await attr_handler.evaluate(config, ctx) is True


async def test_attribute_regex_valid_no_match_deny(attr_handler):
    """Valid pattern that does not match: DENY returns False (no regression)."""
    config = {"attribute": "path", "operator": "regex", "value": r"/items"}
    ctx = _ctx(effect="DENY", path="/other")
    assert await attr_handler.evaluate(config, ctx) is False


# ---------------------------------------------------------------------------
# RateLimitHandler — path_pattern gate keeps fail-open regardless of effect
# ---------------------------------------------------------------------------


@pytest.fixture
def rl_handler() -> RateLimitHandler:
    return RateLimitHandler()


async def test_rate_limit_invalid_path_pattern_fail_open_allow(rl_handler):
    """Invalid path_pattern on a rate_limit condition skips enforcement (fail-open)
    regardless of policy effect — quota availability beats strict enforcement."""
    config = {
        "path_pattern": INVALID_PATTERN,
        "limit": 1,
        "window_seconds": 60,
        "_policy_id": "p1",
        "scope": "principal",
    }
    ctx = _ctx(effect="ALLOW", path="/items", query={})
    ctx.principal_id = "user1"
    # _path_method_matches returns False → handler returns True (skip/allow)
    assert await rl_handler.evaluate(config, ctx) is True


async def test_rate_limit_invalid_path_pattern_fail_open_deny(rl_handler):
    """Same fail-open for a DENY-context rate_limit — the intent is quota, not
    access control; invalid path_pattern should not flip the gate direction."""
    config = {
        "path_pattern": INVALID_PATTERN,
        "limit": 1,
        "window_seconds": 60,
        "_policy_id": "p1",
        "scope": "principal",
    }
    ctx = _ctx(effect="DENY", path="/items", query={})
    ctx.principal_id = "user1"
    assert await rl_handler.evaluate(config, ctx) is True
