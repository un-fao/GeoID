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

def bootstrap_app(
    app: Any,
    include_modules: Optional[List[str]] = None,
    include_extensions: Optional[List[str]] = None,
    include_tasks: Optional[List[str]] = None,
):
    """
    Bootstraps the FastAPI application by discovering and instantiating 
    extensions and modules based on installed entry points.
    """
    from fastapi import FastAPI, Request
    from dynastore.extensions.tools.fast_api import ORJSONResponse
    
    app = cast(FastAPI, app)
    
    # 1. Discover and instantiate foundation
    bootstrap_foundation(include_only=include_modules)
    instantiate_foundation(app.state, include_only=include_modules)

    # 2. Discover and instantiate extensions
    logger.info("--- [extensions/bootstrap.py] Discovering extensions and tasks ---")
    tasks.discover_tasks(include_only=include_tasks)
    extensions.discover_extensions(include_only=include_extensions)
    
    extensions.instantiate_extensions(app, include_only=include_extensions)
    extensions.apply_app_configurations(app)

    # 6. Global Exception Handlers
    @app.exception_handler(InvalidIdentifierError)
    async def invalid_identifier_exception_handler(request: Request, exc: InvalidIdentifierError):
        return ORJSONResponse(
            status_code=400,
            content={"detail": str(exc)},
        )
