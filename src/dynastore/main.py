
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

import logging
from types import SimpleNamespace
import asyncio
from fastapi import FastAPI
import sys
import os
from contextlib import asynccontextmanager
from dynastore import modules, extensions, tasks
from dynastore._version import VERSION, get_build_info
from dynastore.extensions.tools.fast_api import ORJSONResponse
from dynastore.extensions.bootstrap import bootstrap_app
from dynastore.modules.concurrency import set_concurrency_backend
from fastapi.concurrency import run_in_threadpool

# --- Initialize Concurrency Backend ---
# Since this is the FastAPI entry point, we use FastAPI's threadpool runner.
set_concurrency_backend(run_in_threadpool)
# --- Logging Configuration ---
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# --- Combined Application Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the complete application lifecycle in the correct order:
    1. Start Modules (e.g., connect to DB).
    2. Start Web Extensions (e.g., mount API routers).
    3. The application runs.
    4. Shutdown Web Extensions.
    5. Shutdown Modules.
    """
    # The outer context manager initializes foundational modules.
    # They populate `app.state` with core services. TasksModule.lifespan
    # already manages task singletons and the dispatcher internally.
    async with modules.lifespan(app.state):
        logger.info("--- [main.py] Modules are active. ---")
        # Extensions can now reliably access services from modules and task instances.
        async with extensions.lifespan(app):
            # Flush any pending policy/role registrations from extensions
            from dynastore.models.protocols.policies import PermissionProtocol
            pm = modules.get_protocol(PermissionProtocol)
            if pm and hasattr(pm, "flush_pending_registrations"):
                await pm.flush_pending_registrations()
            logger.info("--- [main.py] Web Extensions are active. Application is running. ---")
            yield

    logger.info("--- [main.py] Application shutdown complete. ---")

# --- Main Application Creation ---

app = FastAPI(
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    root_path=os.getenv("API_ROOT_PATH", "/"),
    title=os.getenv("TITLE", "Agro-Informatics Platform - Catalog Services API"),
    description=os.getenv(
        "DESCRIPTION",
        "Multi-tenant, OGC-compliant geospatial data platform implementing OGC API Features (Parts 1-4), "
        "STAC API 1.0.0, Processes, Records, Tiles, Maps, Coverages, and Dimensions with CQL2 filtering, "
        "multi-CRS support, and transaction capabilities.",
    ),
    version=VERSION,
    docs_url=None, # We will serve custom docs
    redoc_url=None, # We will serve custom redoc
    swagger_ui_parameters={"defaultModelsExpandDepth": -1} # Optional: hide models by default
)

@app.get("/health", tags=["Web Health"])
async def health_check():
    info = get_build_info()
    return {
        "name": app.title,
        "description": app.description,
        "version": info["version"],
        "commit": info["commit"],
        "build_time": info["build_time"],
        "status": "ok",
    }

# Custom Swagger UI to include logo
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    from fastapi.openapi.docs import get_swagger_ui_html
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_favicon_url="/web/static/dynastore.png"
    )

# For ReDoc we can also customize similarly.
@app.get("/redoc", include_in_schema=False)
async def custom_redoc_html():
    from fastapi.openapi.docs import get_redoc_html
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_favicon_url="/web/static/dynastore.png"
    )


from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
setup_exception_handlers(app)
bootstrap_app(app)

logger.info("--- [main.py] FastAPI application instance created. ---")


async def run_worker(concurrency: int = 1):
    """
    Initializes the application's modules via their lifespans and runs as a
    long-lived worker process.

    The TasksModule lifespan is responsible for starting the dispatcher and
    queue listener internally — no schema or dispatcher knowledge is needed here.

    Args:
        concurrency: Reserved for future use. The dispatcher concurrency is
                     configured via the TasksModule.
    """
    logger.info("--- [main.py] Initializing worker context (concurrency=%d)... ---", concurrency)

    app_state = SimpleNamespace()

    # 1. Discover all modules based on SCOPE environment variable.
    modules.discover_modules()

    # 2. Instantiate all discovered modules using the shared state object.
    modules.instantiate_modules(app_state)

    # 3. Run module lifespans — TasksModule will start the dispatcher and queue listener.
    async with modules.lifespan(app_state):
        logger.info("--- [main.py] Worker running. Dispatcher managed by TasksModule. ---")
        # Block until manually terminated (SIGTERM will set the shutdown event
        # inside TasksModule and clean up gracefully).
        shutdown_event = asyncio.Event()
        await shutdown_event.wait()

    logger.info("--- [main.py] Worker shut down cleanly. ---")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DynaStore Application Entry Point")
    parser.add_argument("--worker", action="store_true", help="Run as a background worker instead of API server")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Number of concurrent worker processes (worker mode only, default: 1)")
    args = parser.parse_args()

    if args.worker:
        asyncio.run(run_worker(concurrency=args.concurrency))
    else:
        print("This script is intended to be imported by an ASGI server (for API) or run with --worker (for Worker).")