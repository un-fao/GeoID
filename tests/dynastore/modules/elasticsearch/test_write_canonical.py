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

"""TDD tests for Task 5 — canonical doc built at ES write boundary (#1800).

Verifies that ``index_bulk`` (and ``index``) produce a canonical ES ``_source``
shape and use ``_id=geoid`` for every upsert op.

All tests are pure-unit: no live ES cluster, no live PG.  The ES client and
the raw-row reader are both injected as mocks.

Contracts:
  1. For upsert ops ``index_bulk`` calls ``read_canonical_index_inputs`` with
     the batch geoids, then builds the canonical doc via
     ``build_canonical_index_doc``.
  2. The ES ``_id`` in every bulk action line is the geoid (``op.entity_id``).
  3. The ``_source`` carries ``stats.*`` / ``system.*`` populated from the
     resolved sidecars and raw row.
  4. ``properties`` in ``_source`` is user-only (no SYSTEM_FIELD_KEYS).
  5. ``id`` in ``_source`` equals the geoid.
  6. ``_external_id`` tracker is present in ``_source``.
  7. Delete ops are passed through unchanged (``_id`` is still the geoid).
  8. ``write_entities`` uses ``_id=geoid``, not ``stac_doc["id"]``.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.indexer import IndexContext, IndexOp
from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS = frozenset(SYSTEM_FIELD_KEYS)

_GEOID = "019e6318-d99e-7da2-bdd9-1223a0d9cd35"
_EXTERNAL_ID = "EXT-001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStatsSidecar:
    """Sidecar stub producing area + centroid as stats."""

    def producible_computed_names(self):
        return {"area", "centroid"}

    def resolve_computed_value(self, row, name):
        vals = {"area": 99999.0, "centroid": "DEADBEEF"}
        if name in vals:
            return True, vals[name]
        return False, None


def _make_canonical_input(geoid: str = _GEOID, external_id: str = _EXTERNAL_ID):
    """Return a CanonicalIndexInput that build_canonical_index_doc will accept."""
    from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

    row = {
        "geoid": geoid,
        "external_id": external_id,
        "geometry_hash": "abc",
        "attributes_hash": "def",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "area": 99999.0,
        "centroid": "DEADBEEF",
    }
    return CanonicalIndexInput(
        row=row,
        resolved_sidecars=[_FakeStatsSidecar()],
        geometry={"type": "Point", "coordinates": [12.5, 41.9]},
        bbox=None,
        user_properties={"NAME": "Rome"},
        access=None,
    )


def _bulk_ok_response(doc_id: str) -> Dict[str, Any]:
    return {
        "errors": False,
        "items": [{"index": {"_index": "test-items-cat1", "_id": doc_id, "status": 200}}],
    }


def _make_driver():
    """Return a driver instance with enough state for the test."""
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
    driver._index_name_prefix = "ds"
    return driver


def _make_ctx(catalog: str = "cat1", collection: str = "col1"):
    return IndexContext(catalog=catalog, collection=collection)


# ---------------------------------------------------------------------------
# Task 5a — index_bulk: canonical _source + _id=geoid
# ---------------------------------------------------------------------------


class TestIndexBulkCanonical:
    """``index_bulk`` must build canonical docs from the raw-row reader."""

    @pytest.mark.asyncio
    async def test_upsert_produces_canonical_source_and_geoid_id(self):
        """``_source`` has stats/system sections and ``id``==geoid; bulk
        action ``_id`` is also the geoid."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id=_GEOID,
        )

        canonical_input = _make_canonical_input()
        es_mock = AsyncMock()
        es_mock.bulk = AsyncMock(return_value=_bulk_ok_response(_GEOID))
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return _bulk_ok_response(_GEOID)

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={"NAME": {"type": "keyword"}}),
            ),
        ):
            result = await driver.index_bulk(ctx, [op])

        # Bulk body: [action_dict, doc_dict]
        assert len(captured_body) == 2, f"Expected [action, doc]; got {captured_body}"
        action = captured_body[0]
        doc = captured_body[1]

        # _id must be geoid.
        assert action.get("index", {}).get("_id") == _GEOID, (
            f"bulk _id should be geoid; got {action}"
        )

        # routing preserved.
        assert action.get("index", {}).get("routing") == "col1"

        # _source: id==geoid.
        assert doc.get("id") == _GEOID, f"doc id must be geoid; got {doc.get('id')}"

        # _source: _external_id tracker.
        assert "_external_id" in doc, f"_external_id missing from {list(doc.keys())}"
        assert doc["_external_id"] == _EXTERNAL_ID

        # _source: system section present with geoid.
        assert "system" in doc, f"system section missing from {list(doc.keys())}"
        assert doc["system"].get("geoid") == _GEOID

        # _source: stats section present with area.
        assert "stats" in doc, f"stats section missing from {list(doc.keys())}"
        assert doc["stats"].get("area") == 99999.0

        # _source: properties user-only (no SYSTEM_FIELD_KEYS).
        props = doc.get("properties", {})
        for key in SYSTEM_FIELD_KEYS:
            assert key not in props, f"SYSTEM_FIELD_KEY '{key}' leaked into properties"

        assert result.succeeded == 1

    @pytest.mark.asyncio
    async def test_delete_op_passes_through_with_geoid(self):
        """Delete ops are emitted as-is; the delete action ``_id`` is the geoid."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="delete",
            entity_type="item",
            entity_id=_GEOID,
        )

        es_mock = AsyncMock()
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return {"errors": False, "items": [{"delete": {"_id": _GEOID, "status": 200}}]}

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.index_bulk(ctx, [op])

        assert len(captured_body) == 1
        assert captured_body[0].get("delete", {}).get("_id") == _GEOID

    @pytest.mark.asyncio
    async def test_geoid_absent_from_reader_skips_op(self):
        """When the raw-row reader returns no row for a geoid, the op is skipped."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id="ghost-geoid",
        )

        es_mock = AsyncMock()
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return {"errors": False, "items": []}

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                # reader returns empty dict — geoid vanished (race/delete)
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.index_bulk(ctx, [op])

        # No body emitted because the op was skipped.
        assert captured_body == []


# ---------------------------------------------------------------------------
# Task 5b — index (single): canonical _source + _id=geoid
# ---------------------------------------------------------------------------


class TestIndexSingleCanonical:
    """``index`` must also build canonical doc and use ``_id=geoid``."""

    @pytest.mark.asyncio
    async def test_single_upsert_canonical_doc(self):
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id=_GEOID,
        )

        canonical_input = _make_canonical_input()
        es_mock = AsyncMock()
        captured_calls: List[Dict] = []

        async def _fake_index(index, id, body, params=None):
            captured_calls.append({"index": index, "id": id, "body": body, "params": params})

        es_mock.index = _fake_index

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={"NAME": {"type": "keyword"}}),
            ),
        ):
            await driver.index(ctx, op)

        assert len(captured_calls) == 1
        call_kwargs = captured_calls[0]

        # _id must be geoid.
        assert call_kwargs["id"] == _GEOID

        doc = call_kwargs["body"]
        # id in doc == geoid.
        assert doc.get("id") == _GEOID
        # system and stats sections.
        assert "system" in doc
        assert "stats" in doc
        # _external_id tracker.
        assert "_external_id" in doc


# ---------------------------------------------------------------------------
# Task 5c — write_entities: _id=geoid
# ---------------------------------------------------------------------------


class TestWriteEntitiesGeoidId:
    """``write_entities`` must set the bulk ``_id`` to the geoid, not
    ``stac_doc["id"]`` (which may be external_id)."""

    @pytest.mark.asyncio
    async def test_write_entities_bulk_id_is_geoid(self):
        """Bulk action ``_id`` must be the geoid even when the Feature id
        was set by the caller to an external_id."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        driver._index_name_prefix = "ds"

        # Feature whose "id" is an external_id string (not a geoid).
        geoid = "aaaa-1111-bbbb-2222"
        external_id_as_id = "EXT-XYZ"
        feature = MagicMock()
        feature.id = external_id_as_id
        feature.geometry = {"type": "Point", "coordinates": [0, 0]}
        feature.properties = {"NAME": "test"}
        feature.model_dump = MagicMock(
            return_value={
                "id": external_id_as_id,
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {"NAME": "test", "geoid": geoid},
                "geoid": geoid,
            }
        )

        # Driver config with no special conflict policy.
        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={
                    geoid: _make_canonical_input(geoid=geoid, external_id="EXT-XYZ"),
                }),
            ),
        ):
            await driver.write_entities(
                "cat1", "col1",
                [feature],
                context={"geoid": geoid},
            )

        # Find the action line (dict with "index" key containing "_id").
        action_lines = [
            item for item in captured_bulk
            if isinstance(item, dict) and "index" in item
        ]
        assert action_lines, f"No 'index' action in bulk body: {captured_bulk}"
        for action in action_lines:
            doc_id = action["index"]["_id"]
            assert doc_id == geoid, (
                f"bulk _id should be geoid '{geoid}'; got '{doc_id}'"
            )
