#    Copyright 2025 FAO
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

import os

if "DYNASTORE_ENV" not in os.environ:
    os.environ["DYNASTORE_ENV"] = "test"

# Fallback for local testing: point to localhost instead of docker hostname 'db'
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = (
        "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    )

import json
from tests.dynastore.test_utils import generate_test_id
import pytest
import pytest_asyncio
import requests
from httpx import AsyncClient
from typing import Any, Union


def pytest_sessionfinish(session, exitstatus):
    """
    Run database cleanup after the full test session.

    With xdist: pytest calls this in the controller process after ALL workers
    have finished — the only safe place to truncate shared tables without
    racing against in-progress tests.

    Without xdist: called in the main process after all tests finish.
    """
    import asyncio

    # Workers report their own sessionfinish; skip them — only the controller runs cleanup.
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return

    try:
        from tests.dynastore.test_utils.cleanup_db import cleanup_db

        print("\n[POST-SESSION] Running database cleanup...")
        asyncio.run(cleanup_db())
        print("[POST-SESSION] Database cleanup complete.")
    except Exception as e:
        print(f"[POST-SESSION] Cleanup warning (non-fatal): {e}")


def pytest_load_initial_conftests(early_config, parser, args):
    """
    Ensures that if -n (xdist) is used but the plugin is mission, we don't crash.
    Also handles CI environment worker limits if not explicitly specified.
    """
    # Check if xdist is actually available
    try:
        import xdist  # noqa: F401
    except ImportError:
        # If xdist is missing, remove -n/--numprocesses from args to allow sequential run
        # instead of failing with 'unrecognized argument'
        i = 0
        while i < len(args):
            if args[i] == "-n" or args[i].startswith("--numprocesses"):
                # Remove the flag and its value
                args.pop(i)
                if i < len(args) and not args[i].startswith("-"):
                    args.pop(i)
            else:
                i += 1


def pytest_collection_modifyitems(items):
    """
    Patch pytest-asyncio Coroutine items so VS Code's test explorer recognises them.

    pytest-asyncio 1.x wraps async tests in a ``Coroutine`` subclass of
    ``pytest.Function``.  VS Code's Python extension only lists items whose
    ``__class__.__name__ == "Function"``, so async tests become invisible in the
    Testing panel.  Re-stamping the class name fixes discovery without changing
    runtime behaviour (``Coroutine`` already inherits from ``Function``).
    """
    try:
        from pytest_asyncio.plugin import Coroutine  # noqa: F401
    except ImportError:
        return
    for item in items:
        if isinstance(item, Coroutine):
            item.__class__.__name__ = "Function"


def pytest_runtest_setup(item):
    """
    Called before each test is executed.
    Handles marker-based skip logic:

    - ``local_only``: skipped when REMOTE_TEST_EXECUTION=true or CI env is detected.
    - ``gcp``: skipped when GCP Application Default Credentials are not available.
      This replaces the old pattern of stacking ``@local_only`` + ``@gcp`` — tests
      now run wherever valid credentials exist (local *or* CI).
    """
    if item.get_closest_marker("local_only"):
        is_remote = os.getenv("REMOTE_TEST_EXECUTION") == "true"
        is_ci = os.getenv("CI") or os.getenv("GITHUB_ACTIONS")

        if is_remote or is_ci:
            pytest.skip(
                f"Test marked as 'local_only' and running in a {'remote' if is_remote else 'CI'} environment."
            )

    if item.get_closest_marker("gcp"):
        # Attempt to resolve GCP credentials the same way the GCP module does.
        # If credentials are missing the test cannot interact with GCP APIs.
        try:
            from dynastore.modules.gcp.tools.service_account import get_credentials
            creds, _ = get_credentials()
            if not creds:
                pytest.skip("GCP credentials (ADC) not available — skipping GCP test.")
        except Exception as e:
            pytest.skip(f"GCP credentials not available ({e}) — skipping GCP test.")


@pytest.fixture(scope="session", autouse=True)
def wait_for_db(request):
    """
    Block until PostgreSQL accepts real queries (not just TCP connections).

    pg_isready / TCP-level checks pass while the server is still in recovery
    mode. This fixture retries a SELECT 1 so tests only start when the DB is
    actually ready to serve queries.

    Skips the wait when no collected test requires a DB connection (e.g. pure
    unit test runs without app_lifespan or db_engine fixtures).
    """
    needs_db = any(
        "app_lifespan" in item.fixturenames or "db_engine" in item.fixturenames
        for item in request.session.items
    )
    if not needs_db:
        return

    import asyncio
    import time
    import asyncpg

    url = os.getenv("DATABASE_URL", "postgresql://testuser:testpassword@localhost:54320/gis_dev")
    asyncpg_url = url.replace("postgresql+asyncpg://", "postgresql://")

    async def _check():
        conn = await asyncpg.connect(asyncpg_url, timeout=5)
        try:
            await conn.execute("SELECT 1")
        finally:
            await conn.close()

    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            asyncio.run(_check())
            return
        except Exception as exc:
            if attempt < max_attempts - 1:
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Database at {asyncpg_url!r} did not become ready "
                    f"after {max_attempts * 2}s: {exc}"
                ) from exc


@pytest.fixture(scope="session")
def data_id():
    """Returns a unique session ID to avoid naming collisions in the database."""
    return generate_test_id(16)


@pytest.fixture
def dynastore_modules(request):
    """Default modules list. Can be overridden via @pytest.mark.enable_modules."""
    marker = request.node.get_closest_marker("enable_modules")
    if marker:
        return list(marker.args)
    # "gcp" is intentionally excluded from the default set: loading it hooks into
    # catalog lifecycle events and makes real GCS API calls (bucket create/delete)
    # on every catalog operation, adding ~30 s per test.
    # Tests that require GCP behaviour must opt in explicitly:
    #   @pytest.mark.enable_modules("db_config", "db", "catalog", "gcp", ...)
    return ["db_config", "db", "catalog", "stats", "iam", "metadata_postgresql"]


@pytest.fixture
def dynastore_extensions(request):
    """Default extensions list. Can be overridden via @pytest.mark.enable_extensions."""
    marker = request.node.get_closest_marker("enable_extensions")
    if marker:
        return list(marker.args)
    return []


@pytest.fixture
def dynastore_tasks(request):
    """Default tasks list. Can be overridden via @pytest.mark.enable_tasks."""
    marker = request.node.get_closest_marker("enable_tasks")
    if marker:
        return list(marker.args)
    return []


@pytest.fixture(scope="session")
def base_url():
    """Returns the base URL for the DynaStore API."""
    # 1. Respect explicit env var (Full URL)
    url = os.getenv("DYNASTORE_API_URL")
    if url:
        return url.rstrip("/")

    # 2. Build from components (local dev fallback)
    # Support environment specific variable for API_HOST
    # Local: localhost, Docker dev: catalog, Pipeline test: catalog_test
    host = os.getenv("API_HOST", "127.0.0.1")
    port = os.getenv("TCP_PORT", "80")
    # Respect docker-compose port if present
    port = os.getenv("HOST_PORT_CATALOG", port)

    root_path = os.getenv("API_ROOT_PATH", "").rstrip("/")
    return f"http://{host}:{port}{root_path}"


@pytest.fixture(scope="session")
def worker_url():
    """Returns the base URL for the DynaStore Worker."""
    url = os.getenv("DYNASTORE_WORKER_URL")
    if url:
        return url.rstrip("/")

    host = os.getenv("WORKER_HOST", "127.0.0.1")
    port = os.getenv("WORKER_PORT", "81")
    # Respect docker-compose port if present
    port = os.getenv("HOST_PORT_WORKER", port)

    root_path = os.getenv("WORKER_ROOT_PATH", "").rstrip("/")
    if root_path and not root_path.startswith("/"):
        root_path = "/" + root_path

    return f"http://{host}:{port}{root_path}"


@pytest.fixture(scope="session")
def api_client(base_url):
    """Synchronous HTTP client for testing."""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


@pytest_asyncio.fixture(loop_scope="function")
async def async_api_client(base_url):
    """Asynchronous HTTP client for testing."""
    async with AsyncClient(base_url=base_url, timeout=120.0) as client:
        yield client


async def _mint_test_jwt(iam_svc, roles: list[str], subject: str = "test-sysadmin") -> str:
    """Mint a short-lived HS256 JWT for test fixtures."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = await iam_svc.get_jwt_secret()
    payload = {
        "sub": subject,
        "roles": roles,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "dynastore-test",
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


@pytest_asyncio.fixture(loop_scope="function")
async def sysadmin_api_client(base_url, app_lifespan):
    """
    Asynchronous HTTP client for testing with SYSADMIN privileges.
    Mints a JWT with sysadmin role via IamService.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    if not iam_svc:
        raise RuntimeError("AuthenticatorProtocol not available")

    token = await _mint_test_jwt(iam_svc, roles=["sysadmin"])
    headers = {"Authorization": f"Bearer {token}"}

    async with AsyncClient(base_url=base_url, timeout=120.0, headers=headers) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def in_process_client(app_lifespan):
    """
    In-process HTTP client for testing with specific extensions enabled.
    Depends on app_lifespan to start the app with configured modules/extensions.
    """
    from httpx import AsyncClient, ASGITransport

    # app_lifespan yields app.state, which has .app attached
    transport = ASGITransport(app=app_lifespan.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def sysadmin_in_process_client(app_lifespan):
    """
    In-process HTTP client with SYSADMIN privileges.
    Mints a JWT with sysadmin role via IamService.
    """
    from httpx import AsyncClient, ASGITransport
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    headers = {}

    if iam_svc:
        token = await _mint_test_jwt(iam_svc, roles=["sysadmin"])
        headers = {"Authorization": f"Bearer {token}"}

    transport = ASGITransport(app=app_lifespan.app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def authenticated_api_client(app_lifespan, base_url):
    """
    Creates a Principal with 'admin' role and mints a JWT for it.
    No Keycloak required.
    Yields (client, principal) tuple.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol, PrincipalAdminProtocol
    from dynastore.models.auth import Principal

    iam_svc = get_protocol(AuthenticatorProtocol)
    principal_admin = get_protocol(PrincipalAdminProtocol)
    if not iam_svc or not principal_admin:
        pytest.skip("IAM authenticator/principal-admin protocols not available")

    subject_id = f"test-user-{generate_test_id()}"
    principal = Principal(
        provider="test",
        subject_id=subject_id,
        roles=["admin"],
    )
    created = await principal_admin.create_principal(principal)

    token = await _mint_test_jwt(iam_svc, roles=["admin"], subject=subject_id)
    headers = {"Authorization": f"Bearer {token}"}
    async with AsyncClient(base_url=base_url, timeout=120.0, headers=headers) as client:
        yield client, created


@pytest_asyncio.fixture(loop_scope="function")
async def async_worker_client(worker_url):
    """Asynchronous HTTP client for testing."""
    async with AsyncClient(base_url=worker_url, timeout=120.0) as client:
        yield client


async def reset_dynastore_state(engine=None):
    """Drops all singleton instances and resets registries to avoid cross-test contamination."""
    from dynastore import modules, extensions, tasks
    from dynastore.modules.db_config import query_executor
    from dynastore.modules.stats.service import STATS_SERVICE
    from dynastore.extensions import registry
    from dynastore.modules.catalog import catalog_module
    try:
        from dynastore.extensions.gcp import gcp_events
    except ImportError:
        gcp_events = None
    from dynastore.tools.async_utils import signal_bus
    from dynastore.tools.discovery import (
        get_protocol,
        get_protocols,
        _DYNASTORE_PLUGINS,
    )
    from collections import defaultdict
    import asyncio
    import logging

    logger = logging.getLogger(__name__)

    # --- Phase 1: Task Collection and Cancellation ---
    # We collect all background tasks that might be using the current loop/connections.
    pending_tasks = []

    # 4. Reset Query Executor Loop
    query_executor._main_app_loop = None
    query_executor._conn_locks.clear()
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator

    _StartupCoordinator._tasks.clear()
    _StartupCoordinator._lock = asyncio.Lock()


    # 5. Reset Stats Service
    STATS_SERVICE._stats_driver = None
    STATS_SERVICE._drivers = []
    if hasattr(STATS_SERVICE, "_flush_task") and STATS_SERVICE._flush_task:
        if not STATS_SERVICE._flush_task.done():
            STATS_SERVICE._flush_task.cancel()
            pending_tasks.append(STATS_SERVICE._flush_task)
        STATS_SERVICE._flush_task = None

    # 6. Clear Event Listeners (CRITICAL for loop isolation)
    catalog_module._event_listeners = defaultdict(list)
    if gcp_events is not None:
        gcp_events._gcp_event_listeners = defaultdict(list)

    # 7. Clear Module Singletons (CRITICAL FIX)
    catalog_module._module_instance = None

    # 8. Clear Event Manager (CRITICAL for loop isolation)
    from dynastore.modules.catalog.event_service import event_service

    event_service._sync_listeners.clear()
    event_service._async_listeners.clear()
    
    # 8.1 Reset SignalBus (CRITICAL for async test isolation)
    if hasattr(signal_bus, "clear"):
        await signal_bus.clear()
    
    # 8.2 Clear Tasks Table (CRITICAL for test isolation)
    # Use DELETE (RowExclusiveLock) instead of TRUNCATE CASCADE (AccessExclusiveLock)
    # so concurrent SELECT queries from other workers are not blocked.
    if engine:
        from dynastore.modules.db_config.query_executor import DQLQuery, managed_transaction
        from dynastore.modules.db_config.locking_tools import check_table_exists
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.modules.db_config.query_executor import ResultHandler
        from sqlalchemy import text

        schema = get_task_schema()
        async with managed_transaction(engine) as conn:
            for table in ("tasks", "events"):
                if await check_table_exists(conn, table, schema):
                    try:
                        await conn.execute(text(f'DELETE FROM "{schema}".{table}'))
                        logger.info(f"Database: Deleted rows from '{schema}.{table}'.")
                    except Exception as e:
                        logger.warning(f"Database: Failed to delete rows from '{schema}.{table}': {e}")
    if hasattr(event_service, "aggregator") and event_service.aggregator:
        try:
            await event_service.aggregator.stop()
        except Exception as e:
            logger.debug(f"Failed to stop EventService aggregator: {e}")
    event_service._aggregator_started = False

    # 9. Clear Lifecycle Registry (Soft Reset)
    # NOTE: We use soft_clear() to preserve Framework/Module level hooks (decorators)
    # while clearing instance-specific dynamic hooks and cancelling pending tasks.
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
    
    # Track any tasks that are still running for diagnostics
    pending_tasks.extend(
        t for t in lifecycle_registry._active_tasks if not t.done()
    )
    
    lifecycle_registry.soft_clear()
    logger.info("LifecycleRegistry soft-cleared (framework hooks preserved).")

    # 10. Clear Log Service (Crucial for connection isolation)
    from dynastore.modules.catalog.log_manager import LOG_SERVICE

    if (
        hasattr(LOG_SERVICE, "_flush_task")
        and LOG_SERVICE._flush_task
        and not LOG_SERVICE._flush_task.done()
    ):
        LOG_SERVICE._flush_task.cancel()
        pending_tasks.append(LOG_SERVICE._flush_task)

    LOG_SERVICE._flush_task = None
    LOG_SERVICE._buffer = None
    LOG_SERVICE._engine = None

    # Await all cancelled tasks to ensure connections are released
    if pending_tasks:
        try:
            # We use a short timeout because these should cancel quickly
            await asyncio.wait_for(
                asyncio.gather(*pending_tasks, return_exceptions=True), timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for background tasks to cancel during state reset.")
        except Exception as e:
            logger.debug(f"Error while awaiting cancelled tasks: {e}")

    # --- Phase 5: Wiping Global Registries and Discovery (LIFESPAN IS OVER) ---
    # Now that all tasks ARE FINISHED, we can safely clear the framework state.
    
    # Discovery Cache
    get_protocol.cache_clear()
    get_protocols.cache_clear()
    _DYNASTORE_PLUGINS.clear()

    # Module & Task Instances (CRITICAL FIX)
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if hasattr(config, "instance"):
            config.instance = None
            
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if hasattr(config, "instance"):
            config.instance = None
            
    # Also clear the tasks module's internal version just in case
    from dynastore.modules import tasks as tasks_module
    if hasattr(tasks_module, "_DYNASTORE_TASKS"):
        for config in tasks_module._DYNASTORE_TASKS.values():
            if hasattr(config, "instance"):
                config.instance = None

    # Clear discovery cache AGAIN after wiping instances
    get_protocol.cache_clear()
    get_protocols.cache_clear()

    # Extension Instances
    for config in registry._DYNASTORE_EXTENSIONS.values():
        config.instance = None

    # Task Instances
    for config in tasks._DYNASTORE_TASKS.values():
        config.instance = None

    # 10.1 Clear Sidecar Registry (CRITICAL for extension isolation)
    from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
    SidecarRegistry.clear_registry()

    # 11. Clear Caches safely
    from dynastore.modules.catalog import config_service
    from dynastore.modules.tiles import tiles_module
    from dynastore.modules.catalog.catalog_module import CatalogModule
    from dynastore.models.protocols import CatalogsProtocol, AssetsProtocol
    try:
        from dynastore.modules.gcp.gcp_module import GCPModule
    except ImportError:
        GCPModule = None

    def clear_if_cached(obj, attr_name=None):
        target = getattr(obj, attr_name, None) if attr_name else obj
        if target and hasattr(target, "cache_clear"):
            try:
                target.cache_clear()
                # Reset alru_cache event loop binding so it works across
                # function-scoped event loops in pytest.
                # async_lru stores the first loop as a mangled attribute.
                _ALRU_LOOP_ATTR = "_LRUCacheWrapper__first_loop"
                if hasattr(target, _ALRU_LOOP_ATTR):
                    setattr(target, _ALRU_LOOP_ATTR, None)
            except Exception as e:
                logger.debug(f"Failed to clear cache for {attr_name}: {e}")

    # Clear Protocol instance caches
    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc:
        clear_if_cached(catalogs_svc, "_get_catalog_model_cached")
        clear_if_cached(catalogs_svc, "_get_catalog_query")
        
    assets_svc = get_protocol(AssetsProtocol)
    if assets_svc:
        clear_if_cached(assets_svc, "_check_asset_exists_query")

    from dynastore.models.protocols import CollectionsProtocol
    collections_svc = get_protocol(CollectionsProtocol)
    if collections_svc:
        clear_if_cached(collections_svc, "_get_collection_model_cached")
        clear_if_cached(collections_svc, "_resolve_physical_table_cached")
        
    # Config service caches
    clear_if_cached(config_service, "_catalog_config_cache")
    clear_if_cached(config_service, "_collection_config_cache")

    # Tiles module cleanup (clear_registry resets _active_storage_provider too)
    clear_if_cached(tiles_module, "get_tile_resolution_params")
    clear_if_cached(tiles_module, "get_collection_source_srid")
    clear_if_cached(tiles_module, "get_custom_tms")
    clear_if_cached(tiles_module, "list_custom_tms")
    clear_if_cached(tiles_module, "resolve_srid")
    tiles_module.clear_registry()
    clear_if_cached(tiles_module.TilePGPreseedStorage, "check_tile_exists")

    # GCP module caches
    if GCPModule:
        clear_if_cached(GCPModule, "get_self_url")


@pytest.fixture
def db_url():
    """Returns the database URL for the test environment."""
    # Force port 54320 as per user instruction, unless valid override exists.
    # The user explicitly stated "the right port is 54320".
    default_url = "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    url = os.getenv("DATABASE_URL", default_url)
    return url


@pytest_asyncio.fixture(loop_scope="function")
async def db_engine(db_url):
    """Provides a fresh SQLAlchemy engine for each test."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(
        db_url.replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="function")
async def app_lifespan(
    db_url, db_engine, request, dynastore_modules, dynastore_extensions, dynastore_tasks
):
    """
    Fixture that accurately replicates the DynaStore application startup sequence.
    Uses the bootstrap_app function from main.py for perfect synchronization.
    """
    import asyncio
    import os
    import sys
    from unittest.mock import MagicMock, AsyncMock
    from types import SimpleNamespace
    from contextlib import AsyncExitStack
    from fastapi import FastAPI
    from starlette.datastructures import State
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.tools.protocol_helpers import resolve
    from dynastore.modules.gcp.gcp_config import GcpModuleConfig

    # 0. Isolation Reset
    await reset_dynastore_state(engine=db_engine)

    # 1. Set Environment (MUST be done before bootstrap for discovery)
    os.environ["DATABASE_URL"] = db_url
    os.environ["DYNASTORE_TESTING"] = "true"  # Ensure GCP/Cloud Run mocks work
    
    # 2. Mock App Instance (Production-like)
    app = FastAPI()
    app.state = State()  # Fresh state
    
    # Enable global exception handling for test app
    from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
    setup_exception_handlers(app)

    # Set the main loop for query_executor bridge
    from dynastore.modules.db_config.query_executor import set_main_app_loop
    set_main_app_loop(asyncio.get_running_loop())

    # INJECT THE TEST ENGINE BEFORE BOOTSTRAP
    app.state.engine = db_engine

    # 3. Bootstrap Discovery
    from dynastore.extensions.bootstrap import bootstrap_app
    from dynastore import modules, extensions, tasks

    # Define explicit lists for control
    modules_marker = request.node.get_closest_marker("enable_modules")
    if modules_marker:
        modules_list = list(modules_marker.args)
    else:
        # Priority: Fixture -> Env -> Default
        modules_list = dynastore_modules

    extensions_marker = request.node.get_closest_marker("enable_extensions")
    if extensions_marker:
        extensions_list = list(extensions_marker.args)
    else:
        extensions_list = dynastore_extensions

    tasks_marker = request.node.get_closest_marker("enable_tasks")
    if tasks_marker:
        tasks_list = list(tasks_marker.args)
        # Ensure 'tasks' module is enabled if any task is requested
        if "tasks" not in modules_list:
            modules_list.append("tasks")
    else:
        tasks_list = dynastore_tasks

    if modules_list is None:
        modules_list = []
    if extensions_list is None:
        extensions_list = []
        
    # Ensure 'processes' module is enabled if 'processes' extension is enabled
    if "processes" in extensions_list and "processes" not in modules_list:
        modules_list.append("processes")

    # Bootstrap using the explicit lists if provided (or fallback to env)
    bootstrap_app(
        app,
        include_modules=modules_list,
        include_extensions=extensions_list,
        include_tasks=tasks_list
    )

    # Ensure db_config has the engine attached if it was created by bootstrap
    if hasattr(app.state, "db_config"):
        app.state.db_config.engine = db_engine

    # 4. Orchestrate Lifespans
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            modules.lifespan(app.state)
        )

        # 5. Apply Global GCP Config for Tests (Faster visibility / reliable cleanup)
        try:
            import logging
            test_logger = logging.getLogger(__name__)
            configs_svc = resolve(ConfigsProtocol)
            if configs_svc:
                gcp_test_config = GcpModuleConfig(
                    project_id="test-project",
                    region="europe-west1",
                    catalog_visibility_max_retries=30,
                    catalog_visibility_retry_interval=1.0
                )
                await configs_svc.set_config(GcpModuleConfig, gcp_test_config)
                test_logger.info("Tests: Applied GCP Module config overrides.")
        except Exception as e:
             import logging
             logging.getLogger(__name__).warning(f"Tests: Could not apply GCP Module config overrides: {e}")

        # tasks.manage_tasks is now handled via TasksModule.lifespan
        # which is triggered by modules.lifespan(app.state) above.

        try:
            await stack.enter_async_context(extensions.lifespan(app))
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(
                f"Extension lifespan partially failed: {e}"
            )

        # Attach app to state for easy access in tests (e.g. for in-process AsyncClient)
        app.state.app = app
        # Yield app state to the test
        yield app.state


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def app_lifespan_module(request):
    """
    Module-scoped variant of app_lifespan.

    Bootstraps the application ONCE per test file (module) instead of once per
    test function.  This dramatically reduces overhead when many tests in the same
    file share the same module/extension configuration.

    Test isolation is achieved at the DB level via unique catalog/collection IDs
    (uuid4 per test) rather than by restarting the whole application.

    Usage:
        Add ``app_lifespan_module`` as a fixture dependency instead of
        ``app_lifespan``.  All other helpers (catalog_obj, collection_obj, …)
        that reference ``app_lifespan`` should be updated to accept this fixture
        in the local conftest or individual tests.
    """
    import asyncio
    import os
    from contextlib import AsyncExitStack
    from fastapi import FastAPI
    from starlette.datastructures import State
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.tools.protocol_helpers import resolve
    from dynastore.modules.gcp.gcp_config import GcpModuleConfig
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    db_url_val = os.getenv(
        "DATABASE_URL", "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    )

    engine = create_async_engine(
        db_url_val.replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )

    # 0. Isolation Reset (once per module)
    await reset_dynastore_state(engine=engine)

    os.environ["DATABASE_URL"] = db_url_val
    os.environ["DYNASTORE_TESTING"] = "true"

    app = FastAPI()
    app.state = State()

    from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
    setup_exception_handlers(app)

    from dynastore.modules.db_config.query_executor import set_main_app_loop
    set_main_app_loop(asyncio.get_running_loop())

    app.state.engine = engine

    from dynastore.extensions.bootstrap import bootstrap_app
    from dynastore import modules, extensions, tasks

    # Respect enable_modules / enable_extensions / enable_tasks on the module level.
    modules_marker = request.module.__dict__.get("pytestmark", [])

    # Default module list (mirrors the function-scoped fixture)
    modules_list = ["db_config", "db", "catalog", "stats", "iam", "metadata_postgresql"]
    extensions_list = ["features", "web"]
    tasks_list: list = []

    bootstrap_app(
        app,
        include_modules=modules_list,
        include_extensions=extensions_list,
        include_tasks=tasks_list,
    )

    if hasattr(app.state, "db_config"):
        app.state.db_config.engine = engine

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(modules.lifespan(app.state))

        try:
            import logging
            _log = logging.getLogger(__name__)
            configs_svc = resolve(ConfigsProtocol)
            if configs_svc:
                gcp_cfg = GcpModuleConfig(
                    project_id="test-project",
                    region="europe-west1",
                    catalog_visibility_max_retries=30,
                    catalog_visibility_retry_interval=1.0,
                )
                await configs_svc.set_config(GcpModuleConfig, gcp_cfg)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Tests: Could not apply GCP config: {e}")

        try:
            await stack.enter_async_context(extensions.lifespan(app))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Extension lifespan partially failed: {e}")

        app.state.app = app
        yield app.state

    # Module-level teardown
    await reset_dynastore_state(engine=engine)
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="function")
async def task_app_state(db_url, db_engine, dynastore_modules, dynastore_tasks):
    """
    Fixture that replicates the DynaStore generic task environment startup.
    Uses the bootstrap_task_env function from main_task.py.
    """
    import asyncio
    import os
    import sys
    from unittest.mock import MagicMock
    from types import SimpleNamespace
    from contextlib import AsyncExitStack
    from dynastore.tasks.bootstrap import bootstrap_task_env
    from dynastore import modules, tasks
    from dynastore.modules.db_config.db_config import DBConfig
    from dynastore.modules.db_config.query_executor import set_main_app_loop

    # 0. Isolation Reset
    await reset_dynastore_state(engine=db_engine)

    # 1. Set Environment
    os.environ["DATABASE_URL"] = db_url

    # Set the main loop for query_executor bridge
    set_main_app_loop(asyncio.get_running_loop())

    # 2. Setup State
    app_state = SimpleNamespace()
    app_state.engine = db_engine
    DBConfig.database_url = db_url
    app_state.db_config = DBConfig()

    # 3. Bootstrap with specific filters
    bootstrap_task_env(
        app_state,
        include_modules=dynastore_modules,
        include_tasks=dynastore_tasks,
    )

    # 4. Orchestrate Lifespans
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(modules.lifespan(app_state))
        await stack.enter_async_context(tasks.manage_tasks(app_state))

        yield app_state


@pytest.fixture
def test_data_loader(request):
    """
    Fixture that returns a function to load JSON data.
    It searches in:
    1. A 'data' directory relative to the test module.
    2. A global 'tests/data' directory.
    """

    def _loader(filename):
        local_dir = os.path.join(os.path.dirname(request.module.__file__), "data")
        local_path = os.path.join(local_dir, filename)

        if os.path.exists(local_path):
            with open(local_path, "r") as f:
                return json.load(f)

        # Global data directory (tests/data)
        # We find the 'tests' directory by looking up from this file's directory
        tests_dir = os.path.dirname(os.path.abspath(__file__))
        global_path = os.path.join(tests_dir, "data", filename)

        if os.path.exists(global_path):
            with open(global_path, "r") as f:
                return json.load(f)

        raise FileNotFoundError(
            f"Test data file '{filename}' not found in local ({local_dir}) "
            f"or global ({os.path.join(tests_dir, 'data')}) folders."
        )

    return _loader
