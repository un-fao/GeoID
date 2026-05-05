from __future__ import annotations
import sys
import pytest


def test_es_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert ItemsElasticsearchDriver().is_available() is False


def test_es_private_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert ItemsElasticsearchPrivateDriver().is_available() is False
