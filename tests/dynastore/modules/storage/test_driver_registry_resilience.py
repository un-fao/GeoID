#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from __future__ import annotations
import logging
import sys
from typing import Protocol, runtime_checkable


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


def test_misbehaving_driver_does_not_break_registry(caplog):
    from dynastore.tools.discovery import (
        get_protocols, register_plugin, unregister_plugin, _get_protocols_cached,
    )

    @runtime_checkable
    class _Avail(Protocol):
        def is_available(self) -> bool: ...

    class GoodDriver:
        def is_available(self) -> bool: return True

    class BadDriver:
        def is_available(self) -> bool: raise RuntimeError("synthetic")

    good, bad = GoodDriver(), BadDriver()
    register_plugin(good); register_plugin(bad)
    try:
        _get_protocols_cached.cache_clear()
        with caplog.at_level(logging.WARNING, logger="dynastore.tools.discovery"):
            result = get_protocols(_Avail)
    finally:
        unregister_plugin(good); unregister_plugin(bad)
        _get_protocols_cached.cache_clear()

    assert good in result and bad not in result
    assert any("synthetic" in r.message for r in caplog.records)


def test_collection_store_index_keyed_by_snake_case_ref():
    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPostgresqlDriver,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    driver = CollectionPostgresqlDriver()
    register_plugin(driver)
    try:
        DriverRegistry.clear()
        idx = DriverRegistry.collection_store_index()
        assert "collection_postgresql_driver" in idx
        # keys are snake_case class names
        for key in idx:
            assert key == key.lower()
    finally:
        unregister_plugin(driver)
        DriverRegistry.clear()


def test_catalog_store_index_keyed_by_snake_case_ref():
    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        CatalogPostgresqlDriver,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    driver = CatalogPostgresqlDriver()
    register_plugin(driver)
    try:
        DriverRegistry.clear()
        idx = DriverRegistry.catalog_store_index()
        assert "catalog_postgresql_driver" in idx
        for key in idx:
            assert key == key.lower()
    finally:
        unregister_plugin(driver)
        DriverRegistry.clear()


def test_clear_resets_store_indexes():
    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPostgresqlDriver,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    driver = CollectionPostgresqlDriver()
    register_plugin(driver)
    try:
        first = DriverRegistry.collection_store_index()
        DriverRegistry.clear()
        second = DriverRegistry.collection_store_index()
        assert first is not second  # rebuilt after clear
    finally:
        unregister_plugin(driver)
        DriverRegistry.clear()
