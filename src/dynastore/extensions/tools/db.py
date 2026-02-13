from typing import AsyncGenerator


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

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from fastapi import Request

def get_async_engine(request: Request) -> AsyncEngine:
    """
    Gets the asynchronous engine from the application state.
    """
    if not hasattr(request.app.state, 'engine') or not request.app.state.engine:
        raise RuntimeError(
            "Database service not available. The async engine is not initialized."
        )
    return request.app.state.engine


async def get_async_connection(request: Request) -> AsyncGenerator[AsyncConnection, None]:
    """
    FastAPI dependency that provides a transaction-managed asynchronous `Connection`
    to API endpoints. This is the correct pattern for endpoint dependencies.
    """
    if not hasattr(request.app.state, "engine") or request.app.state.engine is None:
        raise RuntimeError("Async database engine not initialized. Ensure 'db' module is loaded.")

    engine: AsyncEngine = request.app.state.engine

    # .begin() starts a transaction and handles commit/rollback automatically.
    async with engine.begin() as conn:
        yield conn