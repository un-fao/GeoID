"""Regression test for #937 — Kibana API key falls back to ``ES_API_KEY``.

Operators on Elastic Cloud get a single API key that authorizes both the ES
cluster and the Kibana endpoint. The fallback lets them set just
``ES_API_KEY`` and have the logs dashboard pick the same value up — no
duplicate secret to keep in sync. ``KIBANA_UPSTREAM_API_KEY`` still wins
when explicitly set, leaving room for a separately-scoped Kibana key.

The resolver lives in ``dynastore.modules.elasticsearch.dashboards_provisioner``
as :func:`kibana_api_key`; the proxy and the logs extension both import it,
so the three call sites cannot drift. The final test pins that import shape.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clear_kibana_env(monkeypatch):
    """Start every case from a clean slate so prior process env doesn't
    mask the fallback we're testing."""
    monkeypatch.delenv("KIBANA_UPSTREAM_API_KEY", raising=False)
    monkeypatch.delenv("ES_API_KEY", raising=False)


def test_kibana_var_wins_over_es_key(monkeypatch):
    """Explicit Kibana key wins so deployments can scope it separately."""
    from dynastore.modules.elasticsearch.dashboards_provisioner import kibana_api_key

    monkeypatch.setenv("KIBANA_UPSTREAM_API_KEY", "kib-key")
    monkeypatch.setenv("ES_API_KEY", "es-key")
    assert kibana_api_key() == "kib-key"


def test_falls_back_to_es_key_when_kibana_unset(monkeypatch):
    """The #937 fix: omitting KIBANA_UPSTREAM_API_KEY reuses ES_API_KEY."""
    from dynastore.modules.elasticsearch.dashboards_provisioner import kibana_api_key

    monkeypatch.setenv("ES_API_KEY", "es-key")
    assert kibana_api_key() == "es-key"


def test_falls_back_to_es_key_when_kibana_blank(monkeypatch):
    """Empty string treated identically to unset (operators clear by blanking)."""
    from dynastore.modules.elasticsearch.dashboards_provisioner import kibana_api_key

    monkeypatch.setenv("KIBANA_UPSTREAM_API_KEY", "")
    monkeypatch.setenv("ES_API_KEY", "es-key")
    assert kibana_api_key() == "es-key"


def test_returns_none_when_neither_set():
    """Local-dev path stays None so headers don't include a bogus auth line."""
    from dynastore.modules.elasticsearch.dashboards_provisioner import kibana_api_key

    assert kibana_api_key() is None


def test_proxy_and_logs_extension_use_the_canonical_resolver():
    """Pin the consolidation: the proxy and the logs extension must import
    :func:`kibana_api_key` from the elasticsearch module, not redefine their
    own. Three previously-duplicated copies were merged in #937's cleanup
    pass; a fresh local copy would let the fallback semantics drift silently.
    """
    from dynastore.modules.elasticsearch.dashboards_provisioner import kibana_api_key
    from dynastore.extensions.logs import dashboards_proxy, log_extension

    assert dashboards_proxy.kibana_api_key is kibana_api_key
    assert log_extension.kibana_api_key is kibana_api_key


def test_dashboards_config_api_key_set_reflects_fallback(monkeypatch):
    """``api_key_set`` in the masked config must be True when only
    ES_API_KEY is set — otherwise the in-page status strip mis-reports
    while auth actually works upstream."""
    import inspect

    from dynastore.extensions.logs import log_extension

    src = inspect.getsource(log_extension.LogExtension._get_dashboards_config)
    assert "kibana_api_key()" in src, (
        "_get_dashboards_config must call the canonical kibana_api_key() so "
        "api_key_set reflects the #937 fallback; reading "
        "KIBANA_UPSTREAM_API_KEY directly would report False while auth "
        "actually works via ES_API_KEY"
    )
