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

"""Regression coverage for the #825 listener-path retirement.

Pins the contract that catalog/collection INDEX propagation has left
the legacy listener path forever. Before #825, ``ElasticsearchModule``
registered async-event listeners on ``CATALOG_*``/``COLLECTION_*``
lifecycle events that enqueued ``ElasticsearchIndexTask`` /
``ElasticsearchDeleteTask`` rows — running in parallel to the canonical
routing-config rails (#820 items / #732 collection / ReindexWorker
catalog) and producing the misleading "Indexing collection …" log line
that originally surfaced #810.

This test catches a regression where someone re-introduces the listener
methods, the dispatch helper imports, or the dead task type.
"""

from __future__ import annotations

import importlib

import pytest

from dynastore.modules.elasticsearch.module import ElasticsearchModule


def test_no_lifecycle_listener_methods() -> None:
    """The four lifecycle-listener methods must not exist on the module."""
    for attr in (
        "_on_catalog_upsert",
        "_on_catalog_delete",
        "_on_collection_upsert",
        "_on_collection_delete",
    ):
        assert not hasattr(ElasticsearchModule, attr), (
            f"ElasticsearchModule.{attr} was reintroduced — this is the "
            "legacy listener path that #825 retired. Catalog secondary-index "
            "propagation now flows via catalog_router → "
            "CATALOG_METADATA_CHANGED → ReindexWorker → the secondary-index "
            "entries of CatalogRoutingConfig.operations[WRITE]; collection "
            "secondary-index via collection_router._dispatch_collection_index "
            "→ IndexDispatcher.fan_out_bulk."
        )


def test_no_indexer_facade_methods() -> None:
    """index_document/delete_document had zero non-test callers and were
    a tail on the listener path — they go with #825."""
    for attr in ("index_document", "delete_document"):
        assert not hasattr(ElasticsearchModule, attr), (
            f"ElasticsearchModule.{attr} was reintroduced — the facade "
            "wrapped the legacy elasticsearch_index/elasticsearch_delete "
            "task types retired in #825."
        )


def test_legacy_task_module_gone() -> None:
    """``dynastore.tasks.elasticsearch.tasks`` housed
    ``ElasticsearchIndexTask`` / ``ElasticsearchDeleteTask`` — only the
    retired listener path imported it. Module must no longer be importable."""
    with pytest.raises(ImportError):
        importlib.import_module("dynastore.tasks.elasticsearch.tasks")


def test_bulk_reindex_still_present() -> None:
    """The Cloud Run Job bulk-reindex entry point survives the retirement."""
    assert hasattr(ElasticsearchModule, "bulk_reindex")
    assert hasattr(ElasticsearchModule, "ensure_index")
