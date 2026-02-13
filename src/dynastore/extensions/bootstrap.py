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
from typing import List, Optional, Any, cast
from dynastore import extensions, tasks
from dynastore.bootstrap import bootstrap_foundation, instantiate_foundation
from dynastore.tools.db import InvalidIdentifierError

logger = logging.getLogger(__name__)

def bootstrap_app(app: Any, modules_list: Optional[List[str]] = None, extensions_list: Optional[List[str]] = None, tasks_list: Optional[List[str]] = None):
    """
    Bootstraps the FastAPI application by discovering and instantiating all extensions.
    """
    from fastapi import FastAPI, Request
    from dynastore.extensions.tools.fast_api import ORJSONResponse
    
    app = cast(FastAPI, app)
    # 1. Discover foundational modules
    bootstrap_foundation(modules_list=modules_list)

    # 2. Discover extensions and tasks
    logger.info("--- [extensions/bootstrap.py] Discovering extensions and tasks... ---")
    tasks.discover_tasks(enabled_tasks=tasks_list, enabled_modules=modules_list)
    extensions.discover_extensions(enabled_extensions=extensions_list, enabled_modules=modules_list)
    from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
    logger.info(f"--- DISCOVERED EXTENSIONS: {list(_DYNASTORE_EXTENSIONS.keys())} ---")
    

    # 3. Instantiate foundational modules
    instantiate_foundation(app.state, modules_list=modules_list)

    # 4. Instantiate extensions
    extensions.instantiate_extensions(app, enabled_extensions=extensions_list)

    # 5. Apply early configurations
    extensions.apply_app_configurations(app)

    # 6. Global Exception Handlers
    @app.exception_handler(InvalidIdentifierError)
    async def invalid_identifier_exception_handler(request: Request, exc: InvalidIdentifierError):
        return ORJSONResponse(
            status_code=400,
            content={"detail": str(exc)},
        )
