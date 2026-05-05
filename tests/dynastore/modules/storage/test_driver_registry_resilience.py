from __future__ import annotations
import sys


def test_es_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver

    class _ConcreteES(ItemsElasticsearchDriver):
        def get_config_service(self):  # stub for pyright; not invoked by is_available
            return None

    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert _ConcreteES().is_available() is False


def test_es_private_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )

    class _ConcretePrivate(ItemsElasticsearchPrivateDriver):
        def get_config_service(self):
            return None

    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert _ConcretePrivate().is_available() is False
