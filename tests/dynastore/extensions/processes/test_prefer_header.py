"""Unit tests for OGC API - Processes Part 1 Prefer-header dispatch.

Pass 3 gap #1: the legacy ``wait=`` token was the only recognized sync
hint, so standards-compliant clients sending ``Prefer: respond-sync``
were silently routed to async. ``_parse_prefer_header`` must honor
both RFC 7240 tokens + the legacy one.
"""

from __future__ import annotations

from dynastore.extensions.processes.processes_service import (
    _parse_prefer_header,
)
from dynastore.modules.processes.models import JobControlOptions


def test_none_returns_none():
    assert _parse_prefer_header(None) is None
    assert _parse_prefer_header("") is None


def test_respond_async_is_honored():
    assert _parse_prefer_header("respond-async") == JobControlOptions.ASYNC_EXECUTE


def test_respond_sync_is_honored():
    assert _parse_prefer_header("respond-sync") == JobControlOptions.SYNC_EXECUTE


def test_legacy_wait_token_is_honored_as_sync():
    assert _parse_prefer_header("wait=0") == JobControlOptions.SYNC_EXECUTE
    assert _parse_prefer_header("wait=10") == JobControlOptions.SYNC_EXECUTE


def test_mixed_case_is_normalized():
    assert _parse_prefer_header("Respond-Sync") == JobControlOptions.SYNC_EXECUTE
    assert _parse_prefer_header("RESPOND-ASYNC") == JobControlOptions.ASYNC_EXECUTE


def test_async_wins_when_both_present():
    # If a client sends both tokens (malformed), we honor async — the safer
    # default, matching the pre-fix behavior for the async branch.
    assert _parse_prefer_header("respond-async, respond-sync") == JobControlOptions.ASYNC_EXECUTE


def test_unknown_preference_returns_none():
    assert _parse_prefer_header("handling=lenient") is None
