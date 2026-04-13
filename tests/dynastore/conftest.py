import pytest
import pytest_asyncio
import os
import asyncio
import uuid
from dynastore.tools.identifiers import generate_geoid, generate_id_hex, generate_task_id

# Must configure testing environment variables before ANY Pydantic models are imported!
os.environ["DYNASTORE_QUEUE_POLL_INTERVAL"] = "0.5"

from dynastore.modules.catalog.models import Catalog, Collection, ItemDataForDB
import sys
from pathlib import Path

# Add the tests directory to sys.path to allow importing cleanup_db
tests_root = str(Path(__file__).parent.parent.parent)
if tests_root not in sys.path:
    sys.path.append(tests_root)


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def db_reset_session():
    """
    Drops database schemas at the start of the test session to ensure a clean state.
    Schema recreation is handled by module lifespans in app_lifespan / task_app_state.
    """
    try:
        from tests.dynastore.test_utils.cleanup_db import cleanup_db as cleanup
    except ImportError:
        print("[DB RESET] Warning: cleanup_db not found, skipping reset.")
        cleanup = None

    if os.environ.get("PYTEST_XDIST_WORKER"):
        print(
            f"[DB RESET] Worker {os.environ.get('PYTEST_XDIST_WORKER')} detected. Skipping global schema reset to prevent conflict."
        )
        return

    if cleanup:
        print("\n[DB RESET] Dropping existing schemas...")
        await cleanup()

    print("[DB RESET] Complete.\n")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_cleanup():
    """
    Cleans up the database schemas before and after the test session.
    """
    try:
        from tests.dynastore.test_utils.cleanup_db import cleanup_db

        await cleanup_db()
        yield
        await cleanup_db()
    except ImportError:
        print("[DB CLEANUP] Warning: cleanup_db not found, skipping cleanup.")
        yield


@pytest.fixture
def catalog_id() -> str:
    """Generates a unique catalog ID."""
    return f"it_{generate_id_hex()}"


@pytest.fixture
def collection_id() -> str:
    """Generates a unique collection ID."""
    return f"col_{generate_id_hex()}"


@pytest.fixture
def item_id() -> str:
    """Generates a unique item ID (UUIDv7 string, matching server-side geoid format)."""
    return generate_geoid()


@pytest.fixture
def task_id() -> uuid.UUID:
    """Generates a unique task ID (UUIDv7)."""
    return generate_task_id()


@pytest.fixture
def config_catalog_data():
    """Generates PG driver config data (sidecars, partitioning, etc.)."""
    from dynastore.modules.storage.driver_config import DriverRecordsPostgresqlConfig

    return DriverRecordsPostgresqlConfig().model_dump()


@pytest.fixture
def catalog_obj(catalog_id, test_data_loader):
    """Returns a Catalog Pydantic object."""
    from dynastore.models.shared_models import Language

    data = test_data_loader("test_catalog.json")
    data["id"] = catalog_id
    # Ensure localized fields
    if isinstance(data.get("title"), str):
        data["title"] = {Language.EN.value: data["title"]}
    if isinstance(data.get("description"), str):
        data["description"] = {Language.EN.value: data["description"]}
    return Catalog(**data)


@pytest.fixture
def catalog_data(catalog_obj):
    """Returns catalog data as dict."""
    return catalog_obj.model_dump(by_alias=True, exclude_none=True)


@pytest.fixture
def collection_obj(collection_id, test_data_loader):
    """Returns a Collection Pydantic object."""
    from dynastore.models.shared_models import Language

    data = test_data_loader("test_collection.json")
    data["id"] = collection_id
    data["catalog_id"] = "it"  # Placeholder, overwritten often
    # Ensure localized fields
    if isinstance(data.get("title"), str):
        data["title"] = {Language.EN.value: data["title"]}
    if isinstance(data.get("description"), str):
        data["description"] = {Language.EN.value: data["description"]}
    return Collection(**data)


@pytest.fixture
def collection_data(collection_obj):
    """Returns collection data as dict."""
    return collection_obj.model_dump(by_alias=True, exclude_none=True)


@pytest.fixture
def item_raw_data(test_data_loader, item_id):
    """Loads raw item data (GeoJSON feature)."""
    data = test_data_loader("test_item.json")
    data["id"] = item_id
    return data


@pytest.fixture
def item_data_for_db(item_id):
    """
    Returns an ItemDataForDB object processed for the database manually to avoid circular imports.
    """
    return ItemDataForDB(
        external_id=item_id,
        attributes={"name": "Rome", "asset_code": "test_asset"},
        wkb_hex_processed="010100000003780b2428fe2840166a4df38ef34440",  # Rome point
        geom_type="Point",
        content_hash="hash",
        asset_code="test_asset",
        bbox_coords=[12.4, 41.8, 12.5, 41.9],
        h3_res0=580321200388407295,  # Rome h3_res0
    )


@pytest_asyncio.fixture
async def catalog_cleaner(app_lifespan):
    """
    Tracks catalog IDs created during a test and force-deletes them on teardown.

    Usage: add ``catalog_cleaner`` to the test signature, then call
    ``catalog_cleaner(catalog_id)`` after each successful catalog creation to
    register the ID for cleanup.  The fixture runs after the test body and any
    ``finally`` blocks, so it catches failures too.
    """
    ids: list = []
    yield ids.append

    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs:
        for cid in ids:
            try:
                await catalogs.delete_catalog(cid, force=True)
            except Exception:
                pass
        await lifecycle_registry.wait_for_all_tasks()


@pytest_asyncio.fixture(autouse=True)
async def wait_for_lifecycle_tasks():
    """
    Ensures that any background tasks scheduled via lifecycle_registry
    are completed before and after the test.
    """
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
    from dynastore.modules.concurrency import await_all_background_tasks
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tasks import get_loaded_task_types

    await lifecycle_registry.wait_for_all_tasks()
    await await_all_background_tasks()

    # Only poll the DB task queue if this test instance has task types registered.
    # Otherwise skip the loop entirely — the common case for most unit/integration tests.
    loaded_types = list(get_loaded_task_types())
    did_poll = False
    if loaded_types:
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.modules.db_config.locking_tools import check_table_exists
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction

        db = get_protocol(DatabaseProtocol)
        if db:
            engine = db.get_any_engine()
            if engine:
                schema = get_task_schema()
                # Poll up to 50 times × 0.05s = 2.5s max
                for _ in range(50):
                    async with managed_transaction(engine) as conn:
                        table_exists = await check_table_exists(conn, "tasks", schema)
                        if not table_exists:
                            break
                        placeholders = ", ".join(f":t_{i}" for i in range(len(loaded_types)))
                        type_params = {f"t_{i}": t for i, t in enumerate(loaded_types)}
                        sql = f"SELECT count(*) FROM \"{schema}\".tasks WHERE status IN ('PENDING', 'ACTIVE') AND task_type IN ({placeholders})"
                        pending_count = await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(conn, **type_params)
                        if pending_count == 0:
                            break
                    did_poll = True
                    await asyncio.sleep(0.05)

    # Minimal settlement sleep only when we actually polled for tasks
    if did_poll:
        await asyncio.sleep(0.05)

    yield

    await lifecycle_registry.wait_for_all_tasks()
    await await_all_background_tasks()

    # Invalidate catalog caches to ensure fresh state for next test
    catalog_svc = get_protocol(CatalogsProtocol)
    if catalog_svc:
        if hasattr(catalog_svc, "_get_catalog_model_cached"):
            catalog_svc._get_catalog_model_cached.cache_clear()
        if hasattr(catalog_svc, "resolve_physical_schema"):
            # If it's the real service with alru_cache
            if hasattr(catalog_svc.resolve_physical_schema, "cache_clear"):
                catalog_svc.resolve_physical_schema.cache_clear()
