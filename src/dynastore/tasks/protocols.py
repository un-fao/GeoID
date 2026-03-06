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
from typing import Protocol, AsyncGenerator, TypeVar, Generic, runtime_checkable, Any
from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigManager
from dynastore.tools.plugin import ProtocolPlugin

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)


class TaskProtocol(ProtocolPlugin[object], HasConfigManager, Generic[DefinitionType, PayloadType, ReturnType]):
    """
    Defines the contract for a DynaStore Background Task.

    Each Task is a ``ProtocolPlugin[object]``, meaning:
    - Its ``lifespan(app_state: object)`` is the single lifecycle hook —
      no separate ``startup`` / ``shutdown`` methods are needed.
    - Its ``priority`` is compared *only* against other tasks (same category).
    - ``is_available()`` allows optional tasks to be skipped when prereqs are absent.

    The ``run`` method handles the actual task payload execution.

    ---
    Example:
    ```python
    class MessageQueueTask(TaskProtocol):
        priority: int = 5

        @asynccontextmanager
        async def lifespan(self, app_state: object):
            self.connection = await connect_to_queue()
            try:
                yield
            finally:
                await self.connection.close()

        async def run(self, payload):
            await self.connection.process(payload)
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the task.
        """
        return cls._registered_name

    @abc.abstractmethod
    async def run(self, payload: PayloadType) -> ReturnType:
        """
        Executes the task's logic.
        """
        ...
