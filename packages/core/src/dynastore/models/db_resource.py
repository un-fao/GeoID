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

"""SQLAlchemy connection / engine type aliases used across the driver layer.

Canonical location for ``DbResource`` and its family — see #1555.  These
aliases depend only on SQLAlchemy; they live here so that
``models/driver_context.py`` can import them at runtime without crossing
into ``modules/``.

The backward-compat re-export in ``modules/db_config/query_executor``
keeps all existing ``from dynastore.modules.db_config.query_executor
import DbResource`` call sites unchanged.
"""

from typing import Union

from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection as SAConnection
from sqlalchemy.orm import Session as SASession
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, AsyncEngine

# --- Type Definitions ---
DbSyncConnection = Union[SAConnection, SASession]
DbAsyncConnection = Union[AsyncConnection, AsyncSession]
DbEngine = Union[Engine, AsyncEngine]
DbConnection = Union[DbSyncConnection, DbAsyncConnection]
DbSyncResource = Union[Engine, DbSyncConnection]
DbAsyncResource = Union[AsyncEngine, DbAsyncConnection]
DbResource = Union[DbSyncResource, DbAsyncResource]

__all__ = [
    "DbSyncConnection",
    "DbAsyncConnection",
    "DbEngine",
    "DbConnection",
    "DbSyncResource",
    "DbAsyncResource",
    "DbResource",
]
