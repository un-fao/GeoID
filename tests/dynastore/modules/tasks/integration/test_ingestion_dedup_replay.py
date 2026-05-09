"""End-to-end dedup integration test — closes followup #2 from PR #420 / #427.

Replays the same Pub/Sub OBJECT_FINALIZE-shaped ingestion request twice
through ``create_task_for_catalog`` (the path GCP event ingestion now
uses, after PR #427 collapsed the bespoke branch in
``gcp_events.py::_trigger_configured_actions``).

Asserts:

* the second call returns ``None`` (dedup hit, by application-layer
  pre-check in ``create_task``)
* exactly one row exists in ``<schema>.tasks`` matching the
  redelivery-keyed ``dedup_key``
* a third call with a different ``generation`` token (i.e. a new asset
  version) creates a second distinct row — the dedup must collapse only
  redeliveries of the *same* version.

The dedup_key formula must remain ``ingestion:{cat}:{coll}:{asset}:{gen}``
(see ``packages/extensions/gcp/src/dynastore/extensions/gcp/gcp_events.py``).

Implementation note — ``task_type`` is a unique synthetic value rather
than ``"ingestion"``: with the ``tasks`` module enabled, the
``BackgroundRunner`` auto-claims real PENDING ingestion rows and would
race the test by mutating ``status`` (PENDING → ACTIVE → FAILED) between
the two ``create_task_for_catalog`` calls. A synthetic type has no
registered runner, so the row stays PENDING for the duration of the
test — the dedup pre-check sees a stable non-terminal row and the
assertion is deterministic. The dedup primitive itself is task-type-
agnostic; pinning the formula is the contract under test.
"""
from __future__ import annotations

import pytest

from tests.dynastore.test_utils import generate_test_id

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate
from dynastore.tools.discovery import get_protocol


def _ingestion_dedup_key(catalog_id: str, collection_id: str, asset_id: str, generation: str) -> str:
    return f"ingestion:{catalog_id}:{collection_id}:{asset_id}:{generation}"


def _ingestion_inputs(catalog_id: str, collection_id: str, asset_id: str, generation: str) -> dict:
    return {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "ingestion_request": {
            "asset_id": asset_id,
            "source_uri": (
                f"gs://test-bucket/collections/{collection_id}/{asset_id}.csv"
                f"#generation={generation}"
            ),
        },
    }


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="serial")
@pytest.mark.timeout(600)
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "tasks", "collection_postgresql", "catalog_postgresql"
)
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_object_finalize_redelivery_collapses_to_single_ingestion_task(
    app_lifespan, sysadmin_in_process_client
):
    in_process_client = sysadmin_in_process_client
    catalog_id = f"cat_dedup_{generate_test_id(10)}"
    collection_id = f"col_dedup_{generate_test_id(10)}"
    asset_id = f"asset_{generate_test_id(8)}"
    generation = "1715250000000001"
    # Synthetic task_type — no registered runner = no dispatcher race.
    task_type = f"ingestion_dedup_test_{generate_test_id(8)}"

    # 1. Provision catalog (real schema is created). The dedup contract is
    # at (schema_name, dedup_key) level — collection creation is not
    # needed for this assertion and would drag in extension drivers.
    resp = await in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "Dedup Test Catalog"},
    )
    assert resp.status_code == 201, resp.text

    catalogs = get_protocol(CatalogsProtocol)
    schema = await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=app_lifespan.engine)
    )
    assert schema, "physical schema must resolve before dedup test"

    dedup_key = _ingestion_dedup_key(catalog_id, collection_id, asset_id, generation)
    inputs = _ingestion_inputs(catalog_id, collection_id, asset_id, generation)

    # 2. First OBJECT_FINALIZE — task created.
    first = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE",
            inputs=inputs,
            collection_id=collection_id,
            dedup_key=dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert first is not None, "first ingestion task must be created"
    assert first.dedup_key == dedup_key

    # 3. Pub/Sub redelivery — identical payload, identical dedup_key.
    #    create_task pre-check must reject the second insert with None.
    second = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE",
            inputs=inputs,
            collection_id=collection_id,
            dedup_key=dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert second is None, (
        "Pub/Sub redelivery of the same OBJECT_FINALIZE must collapse to "
        "exactly one ingestion task — got a second insert. The dedup "
        "pre-check in create_task or the partial unique index regressed."
    )

    # 4. DB-level invariant: exactly one row matches the dedup_key.
    task_schema = tasks_module.get_task_schema()
    count_sql = (
        f'SELECT COUNT(*) FROM "{task_schema}".tasks '
        f'WHERE schema_name = :schema_name '
        f'AND dedup_key = :dedup_key '
        f'AND task_type = :task_type'
    )
    async with app_lifespan.engine.connect() as conn:
        count = await DQLQuery(
            count_sql, result_handler=ResultHandler.SCALAR_ONE
        ).execute(conn, schema_name=schema, dedup_key=dedup_key, task_type=task_type)
    assert count == 1, (
        f"expected exactly 1 task row for dedup_key={dedup_key} "
        f"in schema={schema}, got {count}. Pub/Sub at-least-once delivery "
        f"is now visible to downstream consumers."
    )

    # 5. New generation = new asset version → MUST insert a new row.
    new_generation = "1715250000000002"
    new_dedup_key = _ingestion_dedup_key(catalog_id, collection_id, asset_id, new_generation)
    third = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE",
            inputs=_ingestion_inputs(catalog_id, collection_id, asset_id, new_generation),
            collection_id=collection_id,
            dedup_key=new_dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert third is not None, (
        "A new GCS object generation must produce a fresh ingestion task — "
        "the dedup is per-version, not per-asset."
    )
    assert third.task_id != first.task_id

    async with app_lifespan.engine.connect() as conn:
        total = await DQLQuery(
            f'SELECT COUNT(*) FROM "{task_schema}".tasks '
            f'WHERE schema_name = :schema_name '
            f'AND task_type = :task_type '
            f'AND dedup_key LIKE :prefix',
            result_handler=ResultHandler.SCALAR_ONE,
        ).execute(
            conn,
            schema_name=schema,
            task_type=task_type,
            prefix=f"ingestion:{catalog_id}:{collection_id}:{asset_id}:%",
        )
    assert total == 2, (
        f"expected 2 rows (one per generation), got {total}"
    )


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="serial")
@pytest.mark.timeout(600)
@pytest.mark.parametrize("terminal_status", ["FAILED", "DEAD_LETTER", "COMPLETED"])
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "tasks", "collection_postgresql", "catalog_postgresql"
)
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_terminal_status_releases_dedup_key_for_reingestion(
    app_lifespan, sysadmin_in_process_client, terminal_status
):
    """Closes followup #2b — operator-driven re-ingestion after a terminal release.

    The partial unique index excludes ``COMPLETED``/``FAILED``/``DEAD_LETTER``
    (see ``tasks_module.py:149-150``) and the application-level pre-check in
    ``enqueue`` uses the same ``NOT IN`` set (``tasks_module.py:1172``). Once
    a row reaches a terminal status, its ``dedup_key`` is *released*: the
    next ingestion with the same key must produce a fresh PENDING row.

    This is the contract operators rely on when re-ingesting after a failure
    (manual retry of a DEAD_LETTERed asset, or re-uploading a fixed source
    over the same GCS generation). Synthetic ``task_type`` per #432 lesson.
    """
    in_process_client = sysadmin_in_process_client
    catalog_id = f"cat_release_{generate_test_id(10)}"
    collection_id = f"col_release_{generate_test_id(10)}"
    asset_id = f"asset_{generate_test_id(8)}"
    generation = "1715250000000099"
    task_type = f"ingestion_dedup_release_test_{generate_test_id(8)}"

    resp = await in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "Dedup Release Test Catalog"},
    )
    assert resp.status_code == 201, resp.text

    catalogs = get_protocol(CatalogsProtocol)
    schema = await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=app_lifespan.engine)
    )
    assert schema, "physical schema must resolve"

    dedup_key = _ingestion_dedup_key(catalog_id, collection_id, asset_id, generation)
    inputs = _ingestion_inputs(catalog_id, collection_id, asset_id, generation)
    task_schema = tasks_module.get_task_schema()

    # 1. First ingestion — establishes the locked dedup_key.
    first = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE",
            inputs=inputs,
            collection_id=collection_id,
            dedup_key=dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert first is not None
    first_task_id = first.task_id

    # 2. Sanity: while non-terminal, dedup is still in force.
    blocked = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE",
            inputs=inputs,
            collection_id=collection_id,
            dedup_key=dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert blocked is None, "pre-release dedup must still block redelivery"

    # 3. Flip the first row to terminal status (simulates the natural
    #    progression: claim → run → fail / complete / dead-letter).
    update_sql = (
        f'UPDATE "{task_schema}".tasks '
        f'SET status = :status '
        f'WHERE task_id = :task_id'
    )
    async with app_lifespan.engine.begin() as conn:
        await DQLQuery(update_sql, result_handler=ResultHandler.NONE).execute(
            conn, status=terminal_status, task_id=first_task_id
        )

    # 4. Re-ingestion with the same dedup_key — MUST create a fresh row.
    released = await tasks_module.create_task_for_catalog(
        app_lifespan.engine,
        TaskCreate(
            task_type=task_type,
            caller_id="gcp_event:OBJECT_FINALIZE.retry",
            inputs=inputs,
            collection_id=collection_id,
            dedup_key=dedup_key,
        ),
        catalog_id=catalog_id,
    )
    assert released is not None, (
        f"After flipping the first row to {terminal_status}, the same "
        f"dedup_key must be reusable. The pre-check or partial index "
        f"regressed — operators cannot retry failed ingestions."
    )
    assert released.task_id != first_task_id
    assert released.dedup_key == dedup_key

    # 5. DB-level invariant: two rows now share the dedup_key — one
    #    terminal, one fresh PENDING. The partial index permits this.
    async with app_lifespan.engine.connect() as conn:
        rows = await DQLQuery(
            f'SELECT task_id, status FROM "{task_schema}".tasks '
            f'WHERE schema_name = :schema_name '
            f'AND dedup_key = :dedup_key '
            f'AND task_type = :task_type '
            f'ORDER BY timestamp ASC',
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(
            conn,
            schema_name=schema,
            dedup_key=dedup_key,
            task_type=task_type,
        )
    assert len(rows) == 2, (
        f"expected 2 rows (one {terminal_status}, one PENDING), got {len(rows)}"
    )
    statuses = {r["status"] for r in rows}
    assert statuses == {terminal_status, "PENDING"}, (
        f"expected statuses {{{terminal_status}, PENDING}}, got {statuses}"
    )

