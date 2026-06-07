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

"""Regression tests: no-path inbound item yields correct ``external_id``
in ``build_tenant_feature_doc`` output via the ``_external_id`` stamping seam.

Scenario: a collection with no ``external_id_path`` configured.  The inbound
item's top-level ``id`` is the external_id by convention.  After the PG write
the result Feature carries ``id=geoid``.  The stamp must carry the INBOUND id
as ``_external_id`` so ``build_tenant_feature_doc`` projects it correctly.
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
    build_tenant_feature_doc,
)


class TestNoPathExternalIdStamp:
    """``build_tenant_feature_doc`` external_id projection when stamped via
    ``_external_id`` (the write-boundary seam injected by ``_apply_index_stamp``
    for no-path collections)."""

    def test_no_path_underscore_external_id_stamped(self):
        """When the outbox payload carries ``_external_id`` (stamped at write
        boundary from the inbound feature id), the private doc builder picks
        it up and projects it as the top-level ``external_id`` field."""
        payload = {
            "id": "geo-uuid-123",   # geoid after PG write (id == geoid)
            "_external_id": "user-provided-id",  # stamped from inbound feature.id
            "properties": {"name": "test"},
        }
        doc = build_tenant_feature_doc(
            payload, catalog_id="cat", collection_id="col",
        )
        assert doc["external_id"] == "user-provided-id"
        # geoid is also present under the canonical name.
        assert doc["geoid"] == "geo-uuid-123"

    def test_no_path_no_stamp_falls_back_to_properties_external_id(self):
        """Without ``_external_id`` (no stamp + no path), the builder falls
        back to ``properties.external_id`` when present."""
        payload = {
            "id": "geo-uuid-456",
            "properties": {"external_id": "prop-level-ext"},
        }
        doc = build_tenant_feature_doc(
            payload, catalog_id="cat", collection_id="col",
        )
        assert doc["external_id"] == "prop-level-ext"

    def test_no_path_no_stamp_no_fallback_omits_external_id(self):
        """When there is no ``_external_id`` stamp and no
        ``properties.external_id``, the field is simply omitted."""
        payload = {
            "id": "geo-uuid-789",
            "properties": {"name": "no external_id here"},
        }
        doc = build_tenant_feature_doc(
            payload, catalog_id="cat", collection_id="col",
        )
        assert "external_id" not in doc

    def test_underscore_stamp_no_conflict_with_properties(self):
        """When ``_external_id`` is set and ``properties`` does NOT contain
        ``external_id``, the stamp value is projected correctly.

        NOTE: when both ``_external_id`` and ``properties.external_id`` are
        present, the SYSTEM_FIELD_KEYS propagation loop in
        ``build_tenant_feature_doc`` gives precedence to
        ``properties.external_id`` (a pre-existing behaviour). This test
        covers the common/clean case where only the stamp is set."""
        payload = {
            "id": "geo-uuid-abc",
            "_external_id": "stamp-value",
            "properties": {"name": "no external_id in props"},
        }
        doc = build_tenant_feature_doc(
            payload, catalog_id="cat", collection_id="col",
        )
        assert doc["external_id"] == "stamp-value"
