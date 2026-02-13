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

# dynastore/tasks/protocols.py
from typing import Protocol, AsyncGenerator, TypeVar, Generic
from dynastore.modules.protocols import HasConfigManager

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)

class TaskProtocol(HasConfigManager, Protocol, Generic[DefinitionType, PayloadType, ReturnType]):
    """
    Defines the contract for a DynaStore Background Task.

    This protocol is the blueprint for creating services that run in the
    background, separate from the FastAPI web server. They are ideal for
    message queue consumers, scheduled jobs, or any long-running process.

    Tasks are registered with the `@dynastore_task` decorator and managed by a
    separate runner script. They support both static and instance-based patterns.

    ---
    1. Static Task (for simple, stateless jobs)
    
    A static task does not have a custom `__init__` method. Its `startup`
    and `shutdown` methods are defined as `staticmethod`s. This is useful for
    simple, procedural setup/teardown logic.

    Example:
    ```python
    # in dynastore/tasks/simple_logger/service.py
    from dynastore.tasks import dynastore_task
    import logging

    @dynastore_task
    class SimpleLoggerTask:
        @staticmethod
        async def startup():
            logging.info("Simple logger task is starting up.")

        @staticmethod
        async def shutdown():
            logging.info("Simple logger task is shutting down.")
    ```
    ---
    2. Instance-Based Task (for stateful services)

    An instance-based task has a custom `__init__` method, allowing it to
    maintain state, such as a connection to a message broker or a database.

    Example:
    ```python
    # in dynastore/tasks/message_queue/service.py
    from dynastore.tasks import dynastore_task
    import asyncio
    import logging

    @dynastore_task
    class MessageQueueTask:
        def __init__(self):
            self.connection = None
            self.is_running = True

        async def startup(self):
            logging.info("Connecting to message queue...")
            await asyncio.sleep(1) # Simulate connection
            self.connection = "DUMMY_CONNECTION"
            asyncio.create_task(self.listen_for_messages())

        async def shutdown(self):
            logging.info("Shutting down message listener...")
            self.is_running = False
            if self.connection:
                await asyncio.sleep(1) # Simulate disconnection
                self.connection = None
                logging.info("Disconnected from message queue.")
        
        async def run(self, payload):
            while self.is_running:
                logging.info("Listening for messages...")
                await asyncio.sleep(5)
    ```
    """
    
    async def startup(self) -> None:
        """
        Code to run when the task service starts. Optional.
        """
        ...

    async def shutdown(self) -> None:
        """
        Code to run when the task service shuts down. Optional.
        """
        ...

    @staticmethod
    def get_process_definition() -> DefinitionType:
        """
        If implemented, returns the typed task definition model.
        """
        ...

    async def run(self, payload: PayloadType) -> ReturnType:
        """
        The core execution logic for the task, with a strongly-typed payload.
        """
        ...

    _registered_name: str = "unregistered_task" # Default value, will be set by decorator
    priority: int = 0  # Default priority

    def is_available(self) -> bool:
        """
        Returns whether the task is currently available to provide its protocol capability.
        Used by the discovery mechanism for prioritized fallbacks.
        """
        return True

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the task.
        This class method reads the name set by the @dynastore_task decorator.
        """
        return cls._registered_name


class ProcessTaskProtocol(TaskProtocol[DefinitionType, PayloadType, ReturnType], Protocol[DefinitionType, PayloadType, ReturnType]):
    """
    A specialized TaskProtocol for tasks that explicitly expose an OGC Process definition.
    """
    @staticmethod
    def get_process_definition() -> DefinitionType:
        """
        MUST return the typed task definition model (e.g., OGC Process).
        """
        ...