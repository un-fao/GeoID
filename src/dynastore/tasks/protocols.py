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

import abc
from typing import Protocol, AsyncGenerator, TypeVar, Generic, runtime_checkable, Any, List, Callable, Optional
from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigService
from dynastore.tools.plugin import ProtocolPlugin

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)


@runtime_checkable
class TaskProtocol(HasConfigService, Protocol, Generic[DefinitionType, PayloadType, ReturnType]):
    """
    Defines the contract for a DynaStore Background Task.
    """

    priority: int = 0

    def is_available(self) -> bool:
        """
        Return True if this task is available for execution.
        """
        return True

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """
        Task lifecycle manager.
        """
        yield

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Skip abstract classes and Protocols
        if cls.__name__.endswith("Protocol") or abc.ABC in cls.__bases__ or getattr(cls, "__is_protocol__", False):
            return
        
        try:
            from dynastore.tasks import _register_task
            _register_task(cls)
        except ImportError:
            # Fallback for when tasks module is not yet fully initialized or available
            pass

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the task.
        """
        return getattr(cls, "_registered_name", cls.__name__.lower())

    required_protocols: tuple[type, ...] = ()

    def are_protocols_satisfied(self) -> bool:
        """Return True if every required protocol has at least one provider installed on this service.

        Uses get_all_protocols() (includes is_available=False providers) rather than
        get_protocol() (active-only) so that a temporarily unavailable module (e.g. GCP
        client failed to init) does not cause the service to stop claiming task types it
        owns — the task will fail and retry on this same service instead of being routed
        to a service that fundamentally lacks the module.
        """
        if not self.required_protocols:
            return True
        from dynastore.tools.discovery import get_all_protocols
        return all(len(get_all_protocols(p)) > 0 for p in self.required_protocols)

    @abc.abstractmethod
    async def run(self, payload: PayloadType) -> ReturnType:
        """
        Executes the task's logic.
        """
        ...


def requires(*protocols: type):
    """Class decorator: declare protocols this task requires to be present for dispatch."""
    def decorator(cls):
        cls.required_protocols = protocols
        return cls
    return decorator
