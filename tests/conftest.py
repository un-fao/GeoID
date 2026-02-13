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
    os.environ["DATABASE_URL"] = "postgresql://testuser:testpassword@localhost:54320/gis_dev"

import json
import uuid
import pytest
import pytest_asyncio
import requests
from httpx import AsyncClient
from typing import Any, Union
def pytest_runtest_setup(item):
    """
    Called before each test is executed.
    Handles the 'local_only' marker by skipping the test if running in a CI environment.
    """
    if item.get_closest_marker("local_only"):
        # Explicit remote marking (e.g. from docker-compose.test.yml)
        is_remote = os.getenv("REMOTE_TEST_EXECUTION") == "true"
        # Standard CI environment variables fallback
        is_ci = os.getenv("CI") or os.getenv("GITHUB_ACTIONS")
        
        if is_remote or is_ci:
            pytest.skip(f"Test marked as 'local_only' and running in a {'remote' if is_remote else 'CI'} environment.")

@pytest.fixture(scope="session")
def data_id():
    """Returns a unique session ID to avoid naming collisions in the database."""
    return str(uuid.uuid4())[:8]

@pytest.fixture
def dynastore_modules():
    """Default modules list. Can be overridden in sub-conftest.py."""
    modules_env = os.getenv("DYNASTORE_MODULES", "db_config,db,catalog,stats")
    return [m.strip() for m in modules_env.split(",")] if modules_env else []

@pytest.fixture
def dynastore_extensions():
    """Default extensions list. Can be overridden in sub-conftest.py."""
    extensions_env = os.getenv("DYNASTORE_EXTENSION_MODULES", "")
    return [m.strip() for m in extensions_env.split(",")] if extensions_env else []

@pytest.fixture
def dynastore_tasks():
    """Default tasks list. Can be overridden in sub-conftest.py."""
    tasks_env = os.getenv("DYNASTORE_TASK_MODULES", "")
    return [t.strip() for t in tasks_env.split(",")] if tasks_env else []

@pytest.fixture(scope="session")
def base_url():
    """Returns the base URL for the DynaStore API."""
    # 1. Respect explicit env var (Full URL)
    url = os.getenv("DYNASTORE_API_URL")
    if url:
        return url.rstrip("/")
    
    # 2. Build from components (local dev fallback)
    # Support environment specific variable for API_HOST
    # Local: localhost, Docker dev: api, Pipeline test: api_test
    host = os.getenv("API_HOST", "127.0.0.1")
    port = os.getenv("TCP_PORT", "80")
    # Respect docker-compose port if present
    port = os.getenv("HOST_PORT_API", port)
    
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





@pytest_asyncio.fixture(loop_scope="function")
async def sysadmin_api_client(base_url, app_lifespan):
    """
    Asynchronous HTTP client for testing with SYSADMIN privileges.
    Uses OAuth2 login flow to obtain a Bearer token.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.apikey import ApiKeyProtocol
    
    # Get the apikey service via protocol
    apikey_svc = get_protocol(ApiKeyProtocol)
    if not apikey_svc:
        raise RuntimeError("ApiKeyProtocol not available")
    
    # Get system admin key
    system_key = await apikey_svc.get_system_admin_key()
    
    # Use the system key as Bearer token (it's recognized by middleware)
    headers = {"Authorization": f"Bearer {system_key}"}
    
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
    Uses OAuth2 Bearer token from system admin key.
    """
    from httpx import AsyncClient, ASGITransport
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.apikey import ApiKeyProtocol
    
    # Get the apikey service via protocol
    apikey_svc = get_protocol(ApiKeyProtocol)
    headers = {}
    
    if apikey_svc:
        system_key = await apikey_svc.get_system_admin_key()
        if system_key:
            headers = {"Authorization": f"Bearer {system_key}"}
    
    # Fallback: strictly use environment variable if module not present
    if not headers:
        import os
        system_key = os.getenv("DYNASTORE_SYSTEM_ADMIN_KEY", "test-system-key")
        headers = {"Authorization": f"Bearer {system_key}"}
        
    transport = ASGITransport(app=app_lifespan.app)
    async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def async_worker_client(worker_url):
    """Asynchronous HTTP client for testing."""
    async with AsyncClient(base_url=worker_url, timeout=120.0) as client:
        yield client

def reset_dynastore_state():
    """Drops all singleton instances and resets registries to avoid cross-test contamination."""
    from dynastore import modules, extensions, tasks
    from dynastore.modules.db_config import query_executor
    from dynastore.modules.stats.service import STATS_SERVICE
    from dynastore.extensions import registry
    from dynastore.modules.catalog import catalog_module
    from dynastore.extensions.gcp import gcp_events
    from dynastore.tools.discovery import get_protocol, get_protocols, _DYNASTORE_PROVIDERS
    from collections import defaultdict
    import asyncio

    # 0. Clear Discovery Cache (CRITICAL)
    get_protocol.cache_clear()
    get_protocols.cache_clear()
    _DYNASTORE_PROVIDERS.clear()
    
    # 1. Clear Module Instances
    for config in modules._DYNASTORE_MODULES.values():
        config.instance = None
    
    # 2. Clear Extension Instances
    for config in registry._DYNASTORE_EXTENSIONS.values():
        config.instance = None
        
    # 3. Clear Task Registry
    # We clear the registry to avoid cross-test contamination from dynamic tasks.
    # Discovery will re-run and populate it for each test.
    tasks._DYNASTORE_TASKS.clear()
        
    # 4. Reset Query Executor Loop
    query_executor._main_app_loop = None
    query_executor._conn_locks.clear()
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator
    _StartupCoordinator._tasks.clear()
    _StartupCoordinator._lock = asyncio.Lock()

    
    # 5. Reset Stats Service
    STATS_SERVICE._stats_driver = None
    STATS_SERVICE._drivers = []
    if hasattr(STATS_SERVICE, '_flush_task') and STATS_SERVICE._flush_task:
        if not STATS_SERVICE._flush_task.done():
            STATS_SERVICE._flush_task.cancel()
        STATS_SERVICE._flush_task = None

    # 6. Clear Event Listeners (CRITICAL for loop isolation)
    catalog_module._event_listeners = defaultdict(list)
    gcp_events._gcp_event_listeners = defaultdict(list)

    # 7. Clear Module Singletons (CRITICAL FIX)
    catalog_module._module_instance = None

    # 8. Clear Event Manager (CRITICAL for loop isolation)
    from dynastore.modules.catalog.event_manager import event_manager
    event_manager._sync_listeners.clear()
    event_manager._async_listeners.clear()

    # 9. Clear Lifecycle Registry (Prevent task leaks and initializer accumulation)
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
    # Manually clear internal lists to avoid adding test-only methods to src/
    for list_attr in [
        "_sync_catalog_initializers", "_sync_catalog_destroyers",
        "_sync_collection_initializers", "_sync_collection_destroyers",
        "_async_catalog_initializers", "_async_catalog_destroyers",
        "_async_collection_initializers", "_async_collection_destroyers",
        "_sync_asset_initializers", "_sync_asset_destroyers",
        "_async_asset_initializers", "_async_asset_destroyers"
    ]:
        if hasattr(lifecycle_registry, list_attr):
            getattr(lifecycle_registry, list_attr).clear()
    
    # Cancel active background tasks
    if hasattr(lifecycle_registry, "_active_tasks"):
        for task in list(lifecycle_registry._active_tasks):
            if not task.done():
                task.cancel()
        lifecycle_registry._active_tasks.clear()

    # 10. Clear Log Service (Crucial for connection isolation)
    from dynastore.modules.catalog.log_manager import LOG_SERVICE
    if hasattr(LOG_SERVICE, '_flush_task') and LOG_SERVICE._flush_task and not LOG_SERVICE._flush_task.done():
        LOG_SERVICE._flush_task.cancel()
    
    LOG_SERVICE._flush_task = None
    LOG_SERVICE._buffer = None
    LOG_SERVICE._engine = None

    # 11. Clear Cache
    from dynastore.modules.catalog.asset_manager import AssetManager
    # If possible, we should find the instance and clear it, or clear class-level if any.
    # alru_cache is instance-bound for AssetManager, and since we clear module instance, new one will be fresh.

@pytest.fixture
def db_url():
    """Returns the database URL for the test environment."""
    # Force port 54320 as per user instruction, unless valid override exists.
    # The user explicitly stated "the right port is 54320".
    default_url = "postgresql://db:testuser@testpassword:54320/gis_dev"
    url = os.getenv("DATABASE_URL", default_url)
    return url

@pytest_asyncio.fixture(loop_scope="function")
async def db_engine(db_url):
    """Provides a fresh SQLAlchemy engine for each test."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool
    engine = create_async_engine(db_url.replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture(loop_scope="function")
async def app_lifespan(db_url, db_engine, request, dynastore_modules, dynastore_extensions, dynastore_tasks):
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
    
    # 0. Isolation Reset
    reset_dynastore_state()
    
    # 1. Set Environment (MUST be done before bootstrap for discovery)
    os.environ["DATABASE_URL"] = db_url

    
    # Define explicit lists for control
    # Check for test markers to override modules
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
    else:
        tasks_list = dynastore_tasks


    from dynastore.extensions.bootstrap import bootstrap_app
    from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
    from dynastore import modules, extensions, tasks
    from dynastore.modules.db_config.db_config import DBConfig
    from dynastore.modules.db_config.query_executor import set_main_app_loop
    
    # 2. Mock App Instance (Production-like)
    app = FastAPI()
    app.state = State() # Fresh state
    
    # Enable global exception handling for test app
    setup_exception_handlers(app)
    
    # Set the main loop for query_executor bridge
    set_main_app_loop(asyncio.get_running_loop())
    
    # Pre-populate DBConfig to avoid re-creation issues
    # Note: We manually insert the engine we created in the db_engine fixture
    
    # Bootstrap using the explicit lists if provided (or fallback to env)
    # We can inspect markers here if we want per-test config
    # Note: GCP env vars are now in tests/dynastore/extensions/gcp/conftest.py
    
    # INJECT THE TEST ENGINE BEFORE BOOTSTRAP
    # DBService will detect this existing engine and use it instead of creating a new one.
    app.state.engine = db_engine
    # Also inject into db_config if needed (some legacy access patterns might use it)
    # but primarily app.state.engine is the source of truth for DBService.

    bootstrap_app(app, modules_list=modules_list, extensions_list=extensions_list, tasks_list=tasks_list)
    
    # Ensure db_config has the engine attached if it was created by bootstrap
    if hasattr(app.state, "db_config"):
        app.state.db_config.engine = db_engine
    
    # 4. Orchestrate Lifespans
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(modules.lifespan(app.state, enabled_modules=modules_list))
        
        try:
             await stack.enter_async_context(tasks.manage_tasks(app.state, enabled_tasks=tasks_list))
        except Exception as e:
             import logging
             logging.getLogger(__name__).warning(f"Tasks management lifespan failed: {e}")
        
        try:
            await stack.enter_async_context(extensions.lifespan(app))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Extension lifespan partially failed: {e}")

        # Attach app to state for easy access in tests (e.g. for in-process AsyncClient)
        app.state.app = app
        # Yield app state to the test
        yield app.state

@pytest_asyncio.fixture(loop_scope="function")
async def task_app_state(db_url, db_engine):
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
    reset_dynastore_state()

    # 1. Set Environment
    os.environ["DATABASE_URL"] = db_url
    if not os.getenv("DYNASTORE_MODULES"):
        os.environ["DYNASTORE_MODULES"] = "db_config,db,catalog,gcp,stats"

    # --- [MOCK PSCOPC FOR PROCRASTINATE] ---
    # Moved to tests/dynastore/tasks/procrastinate/conftest.py

    # Set the main loop for query_executor bridge
    set_main_app_loop(asyncio.get_running_loop())

    # 2. Setup State
    app_state = SimpleNamespace()
    app_state.engine = db_engine
    DBConfig.database_url = db_url 
    app_state.db_config = DBConfig()

    # 3. Bootstrap
    bootstrap_task_env(app_state)


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
