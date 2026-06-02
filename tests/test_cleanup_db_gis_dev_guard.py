"""Guard: a non-xdist cleanup must not clobber the canonical shared ``gis_dev``.

Regression fence for #1723 — a non-xdist ``pytest`` run (``PYTEST_XDIST_WORKER``
unset) used to run ``cleanup_db``'s wholesale truncation against the canonical
shared ``gis_dev``, flipping any live dev stack sharing that DB to
deny-by-default 403. ``reset_would_clobber_shared_gis_dev`` is the fail-safe
predicate that now short-circuits that path.
"""
from __future__ import annotations

import pytest

from tests.dynastore.test_utils.cleanup_db import (
    _CANONICAL_SHARED_DB,
    _RESET_OPT_IN_ENV,
    reset_would_clobber_shared_gis_dev,
)

_SHARED = f"postgresql+asyncpg://u:p@localhost:54320/{_CANONICAL_SHARED_DB}"
_WORKER_CLONE = f"postgresql+asyncpg://u:p@localhost:54320/{_CANONICAL_SHARED_DB}_gw0"
_DISPOSABLE = "postgresql+asyncpg://u:p@localhost:54320/my_throwaway"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    # Start each case from a known non-xdist, non-opted-in baseline.
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.delenv(_RESET_OPT_IN_ENV, raising=False)


def test_blocks_shared_gis_dev_on_non_xdist_run():
    assert reset_would_clobber_shared_gis_dev(_SHARED) is True


def test_allows_under_xdist_worker(monkeypatch):
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    assert reset_would_clobber_shared_gis_dev(_SHARED) is False


def test_allows_with_explicit_opt_in(monkeypatch):
    monkeypatch.setenv(_RESET_OPT_IN_ENV, "1")
    assert reset_would_clobber_shared_gis_dev(_SHARED) is False


def test_allows_worker_clone_db_name():
    # A clone name (gis_dev_gw0) is not the canonical shared name → safe.
    assert reset_would_clobber_shared_gis_dev(_WORKER_CLONE) is False


def test_allows_custom_disposable_db():
    assert reset_would_clobber_shared_gis_dev(_DISPOSABLE) is False


def test_opt_in_must_be_exactly_one(monkeypatch):
    # Truthy-but-not-"1" values do not count as opt-in (fail safe).
    monkeypatch.setenv(_RESET_OPT_IN_ENV, "true")
    assert reset_would_clobber_shared_gis_dev(_SHARED) is True
