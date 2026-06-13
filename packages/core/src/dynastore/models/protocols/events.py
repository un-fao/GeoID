#    Copyright 2026 FAO
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

from typing import (
    Any,
    Optional,
    Protocol,
    runtime_checkable,
    Callable,
    Coroutine,
)

Listener = Callable[..., Coroutine[Any, Any, None]]

@runtime_checkable
class EventsProtocol(Protocol):
    """
    Generic protocol for application domain events.
    Allows components to publish and subscribe to events without direct dependencies.
    """
    
    def register(self, event_type: str, listener: Listener):
        """Register an async listener for an event type."""
        ...

    def sync_event_listener(self, event_type: str):
        """Decorator to register a synchronous event listener."""
        ...

    def async_event_listener(self, event_type: str):
        """Decorator to register an asynchronous (background) event listener."""
        ...

    async def emit(
        self,
        event_type: str,
        *args,
        db_resource: Optional[Any] = None,
        raise_on_error: bool = False,
        **kwargs,
    ):
        """
        Emit an event.
        Persists to Outbox if db_resource is provided.
        Fires sync listeners immediately.
        Schedules async listeners.
        """
        ...
