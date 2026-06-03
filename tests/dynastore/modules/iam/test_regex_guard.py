# Tests for #1767 – invalid regex in IAM conditions must never cause a 500.
#
# Covers:
#   Part A eval-time guard: query_match, attribute match (regex op),
#                           rate_limit path_pattern
#   Part B write-time 422:  _validate_condition_regexes helper

import re
import pytest
from unittest.mock import MagicMock

from dynastore.modules.iam.conditions import (
    EvaluationContext,
    QueryParamHandler,
    AttributeMatchHandler,
    RateLimitHandler,
    _safe_search,
    _safe_regex_matches,
    _path_method_matches,
)
from dynastore.modules.iam.models import Condition


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _ctx(*, effect: str = "ALLOW", path: str = "/", method: str = "GET", **kw) -> EvaluationContext:
    return EvaluationContext(
        request=None,
        storage=MagicMock(),
        path=path,
        method=method,
        effect=effect,
        **kw,
    )


# ---------------------------------------------------------------------------
# _safe_search (rate_limit path_pattern – fail-open)
# ---------------------------------------------------------------------------

def test_safe_search_valid_match():
    m = _safe_search(r"/api/v1/.*", "/api/v1/items", site="test")
    assert m is not None


def test_safe_search_valid_no_match():
    m = _safe_search(r"/api/v2/.*", "/api/v1/items", site="test")
    assert m is None


def test_safe_search_invalid_regex_returns_none_no_exception():
    # An unmatched bracket is invalid
    m = _safe_search(r"[invalid", "/api/v1/items", site="test")
    assert m is None  # fail-open: treated as no match


# ---------------------------------------------------------------------------
# _safe_regex_matches (query_match / attribute regex – effect-aware)
# ---------------------------------------------------------------------------

def test_safe_regex_matches_valid_match():
    ctx = _ctx(effect="ALLOW")
    assert _safe_regex_matches(r"foo\d+", "foo42", matcher=re.fullmatch, site="t", ctx=ctx) is True


def test_safe_regex_matches_valid_no_match():
    ctx = _ctx(effect="ALLOW")
    assert _safe_regex_matches(r"bar\d+", "foo42", matcher=re.fullmatch, site="t", ctx=ctx) is False


def test_safe_regex_matches_invalid_allow_effect_returns_false():
    ctx = _ctx(effect="ALLOW")
    result = _safe_regex_matches(r"[bad", "anything", matcher=re.fullmatch, site="t", ctx=ctx)
    assert result is False  # ALLOW policy: skip (non-match)


def test_safe_regex_matches_invalid_deny_effect_returns_true():
    ctx = _ctx(effect="DENY")
    result = _safe_regex_matches(r"[bad", "anything", matcher=re.fullmatch, site="t", ctx=ctx)
    assert result is True  # DENY policy: keep denying (match)


def test_safe_regex_matches_invalid_none_effect_returns_false():
    ctx = _ctx(effect=None)  # type: ignore[arg-type]
    result = _safe_regex_matches(r"[bad", "anything", matcher=re.fullmatch, site="t", ctx=ctx)
    assert result is False  # unknown effect: fail-closed-safe (no grant)


# ---------------------------------------------------------------------------
# QueryParamHandler – invalid pattern, effect-aware
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_query_param_invalid_regex_allow_returns_false():
    h = QueryParamHandler()
    ctx = _ctx(effect="ALLOW", query_params={"format": "json"})
    config = {"param": "format", "pattern": r"[invalid"}
    result = await h.evaluate(config, ctx)
    assert result is False  # non-match -> ALLOW policy skipped


@pytest.mark.asyncio
async def test_query_param_invalid_regex_deny_keeps_deny():
    """Invalid pattern on a DENY-effect context: condition matches (True),
    so the handler does NOT short-circuit, and evaluate() returns True
    (condition passed -> DENY policy applies)."""
    h = QueryParamHandler()
    ctx = _ctx(effect="DENY", query_params={"format": "json"})
    config = {"param": "format", "pattern": r"[invalid"}
    result = await h.evaluate(config, ctx)
    assert result is True  # condition passes -> DENY policy keeps applying


@pytest.mark.asyncio
async def test_query_param_valid_match():
    h = QueryParamHandler()
    ctx = _ctx(effect="ALLOW", query_params={"format": "json"})
    config = {"param": "format", "pattern": r"json|xml"}
    result = await h.evaluate(config, ctx)
    assert result is True


@pytest.mark.asyncio
async def test_query_param_valid_no_match():
    h = QueryParamHandler()
    ctx = _ctx(effect="ALLOW", query_params={"format": "csv"})
    config = {"param": "format", "pattern": r"json|xml"}
    result = await h.evaluate(config, ctx)
    assert result is False


# ---------------------------------------------------------------------------
# AttributeMatchHandler – regex operator, effect-aware
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_attribute_regex_invalid_allow_returns_false():
    h = AttributeMatchHandler()
    ctx = _ctx(effect="ALLOW", path="/api/v1/items")
    config = {"attribute": "path", "operator": "regex", "value": r"[bad"}
    result = await h.evaluate(config, ctx)
    assert result is False  # non-match -> ALLOW policy skipped


@pytest.mark.asyncio
async def test_attribute_regex_invalid_deny_returns_true():
    h = AttributeMatchHandler()
    ctx = _ctx(effect="DENY", path="/api/v1/items")
    config = {"attribute": "path", "operator": "regex", "value": r"[bad"}
    result = await h.evaluate(config, ctx)
    assert result is True  # keep denying


@pytest.mark.asyncio
async def test_attribute_regex_valid_match():
    h = AttributeMatchHandler()
    ctx = _ctx(effect="ALLOW", path="/api/v1/items")
    config = {"attribute": "path", "operator": "regex", "value": r"/api/v1/.*"}
    result = await h.evaluate(config, ctx)
    assert result is True


@pytest.mark.asyncio
async def test_attribute_regex_valid_no_match():
    h = AttributeMatchHandler()
    ctx = _ctx(effect="ALLOW", path="/api/v2/items")
    config = {"attribute": "path", "operator": "regex", "value": r"/api/v1/.*"}
    result = await h.evaluate(config, ctx)
    assert result is False


# ---------------------------------------------------------------------------
# _path_method_matches – rate_limit path_pattern (fail-open)
# ---------------------------------------------------------------------------

def test_path_method_matches_invalid_regex_skips_quota():
    """An invalid path_pattern regex is treated as no-match (returns False),
    which causes RateLimitHandler/MaxCountHandler to skip enforcement (return
    True = request allowed). This is the fail-open semantic for quota gates."""
    ctx = _ctx(path="/api/v1/items")
    config = {"path_pattern": r"[invalid"}
    # Invalid regex -> _safe_search returns None (no match) -> treated as
    # "path did not match" -> returns False (gate skips quota enforcement).
    result = _path_method_matches(config, ctx)
    assert result is False  # path treated as non-matching -> quota skipped


@pytest.mark.asyncio
async def test_rate_limit_handler_invalid_path_pattern_fail_open():
    """End-to-end: invalid path_pattern in rate_limit config should allow
    the request (fail-open) rather than crash."""
    h = RateLimitHandler()
    ctx = _ctx(path="/api/v1/items")
    config = {"path_pattern": r"[invalid", "limit": 10, "window_seconds": 60}
    # _path_method_matches returns False (invalid = no-match) -> handler returns True
    result = await h.evaluate(config, ctx)
    assert result is True  # fail-open: quota not enforced, request allowed


def test_path_method_matches_valid_match():
    ctx = _ctx(path="/api/v1/items")
    config = {"path_pattern": r"/api/v1/.*"}
    assert _path_method_matches(config, ctx) is True


def test_path_method_matches_valid_no_match():
    ctx = _ctx(path="/api/v2/items")
    config = {"path_pattern": r"/api/v1/.*"}
    assert _path_method_matches(config, ctx) is False


# ---------------------------------------------------------------------------
# Part B – _validate_condition_regexes write-time guard
# ---------------------------------------------------------------------------

def test_validate_condition_regexes_import():
    from dynastore.extensions.admin.admin_service import (
        _validate_condition_regexes,
    )
    assert callable(_validate_condition_regexes)


def test_validate_condition_regexes_valid_passes():
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    cond = Condition(type="query_match", config={"param": "format", "pattern": r"json|xml"})
    _validate_condition_regexes([cond])  # must not raise


def test_validate_condition_regexes_invalid_query_match_raises_422():
    from fastapi import HTTPException
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    cond = Condition(type="query_match", config={"param": "format", "pattern": r"[bad"})
    with pytest.raises(HTTPException) as exc_info:
        _validate_condition_regexes([cond])
    assert exc_info.value.status_code == 422
    assert "query_match" in exc_info.value.detail


def test_validate_condition_regexes_invalid_match_regex_op_raises_422():
    from fastapi import HTTPException
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    cond = Condition(
        type="match",
        config={"attribute": "path", "operator": "regex", "value": r"[bad"},
    )
    with pytest.raises(HTTPException) as exc_info:
        _validate_condition_regexes([cond])
    assert exc_info.value.status_code == 422


def test_validate_condition_regexes_invalid_rate_limit_path_raises_422():
    from fastapi import HTTPException
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    cond = Condition(
        type="rate_limit",
        config={"limit": 10, "window_seconds": 60, "path_pattern": r"[bad"},
    )
    with pytest.raises(HTTPException) as exc_info:
        _validate_condition_regexes([cond])
    assert exc_info.value.status_code == 422


def test_validate_condition_regexes_invalid_quota_rate_limit_raises_422():
    from fastapi import HTTPException
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    quota = {"rate_limit": {"limit": 100, "window_seconds": 60, "path_pattern": r"[bad"}}
    with pytest.raises(HTTPException) as exc_info:
        _validate_condition_regexes(None, quota)
    assert exc_info.value.status_code == 422
    assert "quota.rate_limit" in exc_info.value.detail


def test_validate_condition_regexes_none_conditions_passes():
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    _validate_condition_regexes(None, None)  # must not raise


def test_validate_condition_regexes_valid_quota_passes():
    from dynastore.extensions.admin.admin_service import _validate_condition_regexes
    quota = {"rate_limit": {"limit": 100, "window_seconds": 60, "path_pattern": r"/api/.*"}}
    _validate_condition_regexes(None, quota)  # must not raise
