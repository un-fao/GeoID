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

"""Integration coverage for ``ItemsWritePolicy`` conflict behaviour against PG.

Each test provisions a fresh catalog/collection, attaches an
``ItemsWritePolicy`` at the catalog tier *before* the collection is created
(so ``ensure_storage`` provisions any policy-driven physical columns — notably
the ``validity`` tstzrange for temporal versioning), upserts the same identity
twice, then inspects the physical attributes sidecar directly.

The matrix mirrors ``WriteConflictPolicy``:

* ``update``        — second write overwrites the matched row in place (one row,
  same geoid).
* ``new_version``   — second write archives the previous row (closes its
  ``validity`` upper bound) and inserts a fresh geoid, leaving a contiguous,
  non-overlapping temporal history. This is the SCD-2 behaviour the policy
  docstring promises ("archives the old row (sets validity upper bound)").
* ``new_version`` **without** ``validity`` — falls back to ``update`` (the
  policy documents that ``NEW_VERSION`` requires a ``ValiditySpec``).
* ``refuse``        — second write is skipped with a structured rejection; the
  first row survives untouched.
* ``refuse_fail``   — second write raises ``ConflictError`` (batch-aborting).
* ``refuse_return`` — second write is a read-through: the existing row is
  returned and nothing new is inserted.

Identity throughout is ``external_id`` derived from ``properties.code``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    WriteConflictPolicy,
    BatchConflictPolicy,
)
from dynastore.modules.storage.computed_fields import DeriveSpec
from dynastore.modules.storage.validity import ValiditySpec
from dynastore.modules.storage.errors import ConflictError
from dynastore.modules.catalog.models import (
    Catalog,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
)

T1 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2021, 1, 1, tzinfo=timezone.utc)


def _pin_all(config):
    """Re-validate through a dict so every top-level field is marked 'set'.

    Persistence uses ``model_dump(exclude_unset=True)``; constructing a config
    and persisting it directly would drop fields the caller didn't touch. See
    ``test_immutability._pin_all`` for the full rationale.
    """
    return type(config).model_validate(config.model_dump())


def _feature(code: str, name: str, coords=(12.5, 41.9)) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "id": code,
        "geometry": {"type": "Point", "coordinates": list(coords)},
        "properties": {"code": code, "name": name},
    }


async def _provision(
    catalogs: CatalogsProtocol,
    configs: ConfigsProtocol,
    catalog_id: str,
    collection_id: str,
    policy: ItemsWritePolicy,
) -> None:
    """Fresh catalog + collection with ``policy`` attached at the catalog tier.

    The policy is set BEFORE ``create_collection`` so that ``ensure_storage``
    provisions every policy-driven physical column (e.g. the ``validity``
    tstzrange) as the collection's storage is built.
    """
    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog(Catalog(id=catalog_id, title=catalog_id))
    await configs.set_config(
        ItemsWritePolicy,
        _pin_all(policy),
        catalog_id=catalog_id,
        check_immutability=False,
    )
    await catalogs.create_collection(
        catalog_id,
        Collection(
            id=collection_id,
            title=collection_id,
            description="write-policy integration test",
            extent=Extent(
                spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
                temporal=TemporalExtent(interval=[[None, None]]),
            ),
        ),
    )


async def _attr_rows(
    catalogs: CatalogsProtocol,
    engine,
    catalog_id: str,
    collection_id: str,
    external_id: str,
    *,
    with_validity: bool = False,
) -> List[Dict[str, Any]]:
    """Read the attributes-sidecar rows for ``external_id`` directly from PG.

    Returns geoid (and, when ``with_validity``, the parsed ``validity`` lower /
    upper bounds) ordered oldest-first. Bypasses every read-time validity
    filter so archived versions are visible.
    """
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    hub_table = await catalogs.resolve_physical_table(catalog_id, collection_id)
    attr_table = f"{hub_table}_attributes"
    if with_validity:
        sql = (
            f'SELECT geoid::text AS geoid, '
            f'lower(validity) AS valid_from, upper(validity) AS valid_to '
            f'FROM "{phys_schema}"."{attr_table}" '
            f'WHERE external_id = :ext '
            f'ORDER BY lower(validity) NULLS FIRST'
        )
    else:
        sql = (
            f'SELECT geoid::text AS geoid '
            f'FROM "{phys_schema}"."{attr_table}" '
            f'WHERE external_id = :ext ORDER BY geoid'
        )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, ext=external_id
        )


# ---------------------------------------------------------------------------
# update — overwrite in place
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_overwrites_in_place(app_lifespan, catalog_id, collection_id):
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        derive=DeriveSpec(external_id="properties.code"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))
    after_first = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    assert len(after_first) == 1
    geoid_v1 = after_first[0]["geoid"]

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha-renamed"))
    after_second = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    # In-place: still exactly one row, and it kept the same geoid.
    assert len(after_second) == 1, "update must not append a new row"
    assert after_second[0]["geoid"] == geoid_v1, "update must reuse the geoid"


# ---------------------------------------------------------------------------
# new_version — archive previous (close validity) + insert fresh geoid
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_version_closes_previous_and_opens_new(
    app_lifespan, catalog_id, collection_id
):
    """The crux: re-ingesting an existing identity under ``new_version`` must
    keep BOTH versions, mint a fresh geoid for the new one, and CLOSE the
    previous version's validity upper bound so the history is non-overlapping.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.NEW_VERSION,
        derive=DeriveSpec(external_id="properties.code"),
        validity=ValiditySpec(start_from="context"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    # v1 valid from T1; v2 valid from T2 (drives both the new lower bound and
    # the archived row's closing upper bound).
    await catalogs.upsert(
        catalog_id, collection_id, _feature("A", "Alpha-v1"),
        processing_context={"valid_from": T1},
    )
    await catalogs.upsert(
        catalog_id, collection_id, _feature("A", "Alpha-v2", coords=(13.0, 42.0)),
        processing_context={"valid_from": T2},
    )

    rows = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A",
        with_validity=True,
    )

    assert len(rows) == 2, "new_version must retain both the old and new rows"
    old, new = rows[0], rows[1]
    assert old["geoid"] != new["geoid"], "new_version must mint a fresh geoid"

    # Old version: opened at T1, CLOSED at T2 (the archive step).
    assert old["valid_from"] == T1
    assert old["valid_to"] is not None, (
        "the previous version's validity upper bound must be CLOSED on "
        "new_version (close_on_new_version=True) — found an open-ended old row"
    )
    assert old["valid_to"] == T2, "old version must close exactly at the new version's start"

    # New version: opened at T2, still open (no upper bound).
    assert new["valid_from"] == T2
    assert new["valid_to"] is None, "the new (current) version must stay open-ended"


@pytest.mark.asyncio
async def test_new_version_without_validity_falls_back_to_update(
    app_lifespan, catalog_id, collection_id
):
    """``NEW_VERSION`` requires a ``ValiditySpec``; with ``validity=None`` the
    policy documents a fall-back to ``UPDATE`` — so a re-ingest overwrites in
    place rather than versioning."""
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.NEW_VERSION,
        derive=DeriveSpec(external_id="properties.code"),
        validity=None,
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))
    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha-2"))
    rows = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    assert len(rows) == 1, "new_version without validity must not version (update fallback)"


# ---------------------------------------------------------------------------
# refuse family
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_skips_duplicate(app_lifespan, catalog_id, collection_id):
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE,
        derive=DeriveSpec(external_id="properties.code"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))
    before = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    geoid_v1 = before[0]["geoid"]

    # A single-feature refuse is surfaced as a structured rejection by the
    # service layer (the per-row IngestionReport path) rather than propagating
    # as an exception — the duplicate is simply not written.
    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha-2"))

    after = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    assert len(after) == 1, "refuse must not insert the duplicate"
    assert after[0]["geoid"] == geoid_v1, "the original row must be untouched"


@pytest.mark.asyncio
async def test_refuse_fail_raises_conflict(app_lifespan, catalog_id, collection_id):
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE_FAIL,
        derive=DeriveSpec(external_id="properties.code"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))
    with pytest.raises(ConflictError):
        await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha-2"))

    after = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    assert len(after) == 1, "refuse_fail must abort without inserting"


@pytest.mark.asyncio
async def test_refuse_return_is_idempotent(app_lifespan, catalog_id, collection_id):
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE_RETURN,
        derive=DeriveSpec(external_id="properties.code"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))
    before = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    geoid_v1 = before[0]["geoid"]

    # Read-through: no error, nothing new written.
    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha-2"))
    after = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "A"
    )
    assert len(after) == 1, "refuse_return must not insert a new row"
    assert after[0]["geoid"] == geoid_v1


# ---------------------------------------------------------------------------
# batch-level guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_batch_aborts_whole_batch(app_lifespan, catalog_id, collection_id):
    """``on_batch_conflict=refuse_batch`` rejects the entire ingest batch when
    any incoming feature collides with an existing identity."""
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    policy = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        on_batch_conflict=BatchConflictPolicy.REFUSE,
        derive=DeriveSpec(external_id="properties.code"),
    )
    await _provision(catalogs, configs, catalog_id, collection_id, policy)

    # Seed an existing row that the second batch will collide with.
    await catalogs.upsert(catalog_id, collection_id, _feature("A", "Alpha"))

    # A batch containing a fresh "B" and a colliding "A" must be rejected whole.
    with pytest.raises(ConflictError):
        await catalogs.upsert(
            catalog_id,
            collection_id,
            [_feature("B", "Bravo"), _feature("A", "Alpha-dup")],
        )

    rows_b = await _attr_rows(
        catalogs, app_lifespan.engine, catalog_id, collection_id, "B"
    )
    assert len(rows_b) == 0, "refuse_batch must roll back the whole batch (B not written)"
