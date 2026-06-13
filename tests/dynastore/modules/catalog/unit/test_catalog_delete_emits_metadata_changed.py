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

"""Regression coverage for #825: catalog_service.delete_catalog must
emit ``CATALOG_METADATA_CHANGED`` so ReindexWorker fans the delete out
to the secondary-index entries of ``CatalogRoutingConfig.operations[WRITE]``
(the canonical ES cleanup path that replaces the retired
``ElasticsearchModule._on_catalog_delete`` listener).

Asserts at source-shape level: both the soft- and hard-delete branches
of ``catalog_service.delete_catalog`` contain a
``CatalogEventType.CATALOG_METADATA_CHANGED`` emit with the expected
operation payload. Source-shape rather than runtime because a true
runtime assertion requires standing up the full
``managed_transaction``/SQL stack — overkill for pinning that the line
stays present.
"""

from __future__ import annotations

import inspect

from dynastore.modules.catalog import catalog_service


def _delete_catalog_source() -> str:
    return inspect.getsource(catalog_service.CatalogService.delete_catalog)


def test_soft_delete_emits_metadata_changed_with_soft_delete_operation() -> None:
    src = _delete_catalog_source()
    assert 'CatalogEventType.CATALOG_METADATA_CHANGED' in src, (
        "delete_catalog no longer emits CATALOG_METADATA_CHANGED — the #825 "
        "canonical-path extension was reverted. Re-add the inline "
        "emit_event(CATALOG_METADATA_CHANGED, operation='soft_delete'|'delete') "
        "calls at the soft-delete branch (return True path) AND after the "
        "CATALOG_HARD_DELETION emit."
    )
    assert '"operation": "soft_delete"' in src, (
        "soft-delete branch must emit CATALOG_METADATA_CHANGED with "
        "operation='soft_delete' so ReindexWorker._dispatch_one routes to "
        "driver.delete_catalog_metadata(soft=True)."
    )


def test_hard_delete_emits_metadata_changed_with_delete_operation() -> None:
    src = _delete_catalog_source()
    assert '"operation": "delete"' in src, (
        "hard-delete branch must emit CATALOG_METADATA_CHANGED with "
        "operation='delete' so ReindexWorker._dispatch_one routes to "
        "driver.delete_catalog_metadata(soft=False)."
    )
