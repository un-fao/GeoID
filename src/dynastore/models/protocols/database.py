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

from typing import Protocol, Optional, Any, runtime_checkable

@runtime_checkable
class DatabaseProtocol(Protocol):
    """
    Protocol for centralized database engine access, abstracting the distinction
    between sync and async engines and facilitating decoupled discovery.
    """
    
    @property
    def engine(self) -> Any:
        ...

    @property
    def async_engine(self) -> Optional[Any]:
        ...

    @property
    def sync_engine(self) -> Optional[Any]:
        ...

    def get_any_engine(self) -> Optional[Any]:
        ...

# --- Generic Database Type Abstractions ---

@runtime_checkable
class DbResourceProtocol(Protocol):
    """Generic protocol for any database resource (Engine, Connection, Session)."""
    ...

@runtime_checkable
class DbEngineProtocol(DbResourceProtocol, Protocol):
    """Generic protocol for a database Engine."""
    def connect(self) -> Any: ...
    def begin(self) -> Any: ...

@runtime_checkable
class DbConnectionProtocol(DbResourceProtocol, Protocol):
    """Generic protocol for a database Connection or Session."""
    def execute(self, statement: Any, parameters: Optional[Any] = None) -> Any: ...
    def begin(self) -> Any: ...
    def begin_nested(self) -> Any: ...
    def in_transaction(self) -> bool: ...
    @property
    def info(self) -> dict: ...

@runtime_checkable
class DbAsyncResourceProtocol(Protocol):
    """Generic protocol for any asynchronous database resource."""
    ...

@runtime_checkable
class DbAsyncEngineProtocol(DbAsyncResourceProtocol, Protocol):
    """Generic protocol for an asynchronous database Engine."""
    def connect(self) -> Any: ...
    def begin(self) -> Any: ...

@runtime_checkable
class DbAsyncConnectionProtocol(DbAsyncResourceProtocol, Protocol):
    """Generic protocol for an asynchronous database Connection or Session."""
    async def execute(self, statement: Any, parameters: Optional[Any] = None) -> Any: ...
    def begin(self) -> Any: ...
    def begin_nested(self) -> Any: ...
    def in_transaction(self) -> bool: ...
    @property
    def info(self) -> dict: ...
    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any: ...
