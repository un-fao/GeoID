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
from contextlib import AsyncExitStack, asynccontextmanager
from typing import List

from fastapi import APIRouter, FastAPI

# Re-export key components for compatibility with main.py
from .protocols import ExtensionProtocol
from .registry import (
    ExtensionConfig,
    discover_extensions,
    dynastore_extension,
    get_extension_instance,
    get_extension_instance_by_class,
    instantiate_extensions,
)
from .documentation import (
    configure_swagger_ui,
    setup_global_help_endpoint,
    enrich_extension_metadata
)

logger = logging.getLogger(__name__)

def apply_app_configurations(app: FastAPI):
    """
    Applies global configurations and triggers extension specific hooks.
    """
    configs: List[ExtensionConfig] = getattr(app.state, "ordered_configs", [])
    logger.info("--- Applying pre-startup app configurations from extensions (e.g., middleware) ---")

    # 1. Apply global Swagger UI enhancements
    configure_swagger_ui(app)

    # 2. Apply extension-specific configurations
    for config in configs:
        if config.instance:
            configure_hook = getattr(config.instance, "configure_app", None)
            if callable(configure_hook):
                try:
                    configure_hook(app)
                    logger.info(f"Applied app configuration from '{config.cls.__name__}'.")
                except Exception:
                    logger.error(f"Failed during configure_app of '{config.cls.__name__}'.", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    A FastAPI lifespan manager that orchestrates the startup and shutdown
    of all enabled extensions.
    """
    configs: List[ExtensionConfig] = getattr(app.state, "ordered_configs", [])
    ordered_modules: List[str] = getattr(app.state, "ordered_modules", [])

    if not ordered_modules:
        logger.warning("Lifespan: No extension modules enabled.")
        yield
        return

    # 1. Setup the global help infrastructure
    setup_global_help_endpoint(app)

    logger.info(f"Activating {len(configs)} extensions in order: {ordered_modules}")

    async with AsyncExitStack() as stack:
        # --- PHASE 2: Lifespans ---
        logger.info("--- Phase 2: Executing all extension lifespans ---")
        for config in configs:
            if config.instance is None: continue
            lifespan_cm = getattr(config.instance, "lifespan", None)
            if callable(lifespan_cm):
                try:
                    await stack.enter_async_context(lifespan_cm(app))
                    logger.info(f"Lifespan for '{config.cls.__name__}' entered successfully.")
                except Exception:
                    logger.error(f"CRITICAL: Lifespan for '{config.cls.__name__}' failed on startup.", exc_info=True)

        # --- PHASE 3: Routers & Docs ---
        logger.info("--- Phase 3: Mounting all extension routers ---")
        for config in configs:
            if config.instance is None: continue
            
            router = getattr(config.instance, "router", None)
            
            if isinstance(router, APIRouter):
                # 1. Enrich Metadata (READMEs, Tags, Help Links)
                enrich_extension_metadata(app, config, router)
                
                # 2. Mount Router
                extension_name = config.cls._registered_name
                if not router.tags:
                    app.include_router(router, tags=[extension_name])
                else:
                    app.include_router(router)
                
                logger.info(f"Mounted router for '{config.cls.__name__}' at prefix '{router.prefix}'.")
        
        # --- Force OpenAPI Schema Refresh ---
        app.openapi_schema = None
        
        yield
    
    logger.info("Application shutting down.")