"""Regression test for #937 — KIBANA_UPSTREAM_API_KEY falls back to ES_API_KEY.

Operators on Elastic Cloud get a single API key that authorizes both the ES
cluster and the Kibana endpoint. The fallback lets them set just
``ES_API_KEY`` and have the logs dashboard pick the same value up — no
duplicate secret to keep in sync. ``KIBANA_UPSTREAM_API_KEY`` still wins
when explicitly set, leaving room for a separately-scoped Kibana key.

Three modules read this value; this test pins all three.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clear_kibana_env(monkeypatch):
    """Start every case from a clean slate so prior process env doesn't
    mask the fallback we're testing."""
    monkeypatch.delenv("KIBANA_UPSTREAM_API_KEY", raising=False)
    monkeypatch.delenv("ES_API_KEY", raising=False)


def _reload():
    """Reload all three modules so module-level globals (if any) refresh."""
    import importlib

    from dynastore.extensions.logs import dashboards_proxy, log_extension
    from dynastore.modules.elasticsearch import dashboards_provisioner

    importlib.reload(dashboards_provisioner)
    importlib.reload(dashboards_proxy)
    importlib.reload(log_extension)
    return dashboards_provisioner, dashboards_proxy, log_extension


def test_kibana_var_wins_over_es_key(monkeypatch):
    """Explicit Kibana key wins so deployments can scope it separately."""
    monkeypatch.setenv("KIBANA_UPSTREAM_API_KEY", "kib-key")
    monkeypatch.setenv("ES_API_KEY", "es-key")
    provisioner, proxy, logs = _reload()
    assert provisioner._api_key() == "kib-key"
    assert proxy._api_key() == "kib-key"
    assert logs._api_key() == "kib-key"


def test_falls_back_to_es_key_when_kibana_unset(monkeypatch):
    """The #937 fix: omitting KIBANA_UPSTREAM_API_KEY reuses ES_API_KEY."""
    monkeypatch.setenv("ES_API_KEY", "es-key")
    provisioner, proxy, logs = _reload()
    assert provisioner._api_key() == "es-key"
    assert proxy._api_key() == "es-key"
    assert logs._api_key() == "es-key"


def test_falls_back_to_es_key_when_kibana_blank(monkeypatch):
    """Empty string treated identically to unset (operators clear by blanking)."""
    monkeypatch.setenv("KIBANA_UPSTREAM_API_KEY", "")
    monkeypatch.setenv("ES_API_KEY", "es-key")
    provisioner, proxy, logs = _reload()
    assert provisioner._api_key() == "es-key"
    assert proxy._api_key() == "es-key"
    assert logs._api_key() == "es-key"


def test_returns_none_when_neither_set(monkeypatch):
    """Local-dev path stays None so headers don't include a bogus auth line."""
    provisioner, proxy, logs = _reload()
    assert provisioner._api_key() is None
    assert proxy._api_key() is None
    assert logs._api_key() is None


def test_dashboards_config_api_key_set_reflects_fallback(monkeypatch):
    """``api_key_set`` in the masked config must be True when only
    ES_API_KEY is set — otherwise the in-page status strip mis-reports
    while auth actually works upstream."""
    monkeypatch.setenv("ES_API_KEY", "es-key")
    _, _, logs = _reload()

    import inspect

    src = inspect.getsource(logs.LogExtension._get_dashboards_config)
    assert "_api_key()" in src, (
        "_get_dashboards_config must call _api_key() so api_key_set reflects "
        "the #937 fallback semantics; reading KIBANA_UPSTREAM_API_KEY "
        "directly would report False while auth actually works via ES_API_KEY"
    )
