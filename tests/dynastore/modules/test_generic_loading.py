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

import pytest
from typing import Protocol, runtime_checkable, Any
from contextlib import asynccontextmanager

from dynastore.modules import _DYNASTORE_MODULES, ModuleConfig
from dynastore.modules.protocols import ModuleProtocol
from dynastore.tools.discovery import get_protocol


# ---- Test Protocol ----
class GreeterProtocol(Protocol):
    """A test protocol for lookup verification."""
    def greet(self, name: str) -> str: ...


# ---- Mock Module implementing GreeterProtocol ----
class MockGreeterModule(ModuleProtocol):
    """Mock module used to test generic protocol lookup via get_protocol()."""
    _registered_name = "test_generic_loading"

    def greet(self, name: str) -> str:
        return f"Hello, {name}!"


@pytest.mark.asyncio
async def test_generic_module_lookup():
    """
    Verifies that get_protocol correctly discovers modules by protocol type.
    We manually insert a mock module into _DYNASTORE_MODULES to simulate registration.
    """
    original_modules = _DYNASTORE_MODULES.copy()

    try:
        # Manually register the mock module
        instance = MockGreeterModule()
        _DYNASTORE_MODULES["test_generic_loading"] = ModuleConfig(
            cls=MockGreeterModule, instance=instance
        )

        # Lookup by protocol type — GreeterProtocol is a structural Protocol
        # get_protocol will use isinstance() which applies structural typing here
        @runtime_checkable
        class RuntimeGreeterProtocol(Protocol):
            def greet(self, name: str) -> str: ...

        provider = get_protocol(RuntimeGreeterProtocol)

        assert provider is not None
        assert isinstance(provider, RuntimeGreeterProtocol)
        assert provider.greet("World") == "Hello, World!"  # type: ignore[union-attr]

        # Test: non-existent protocol returns None
        @runtime_checkable
        class MissingProtocol(Protocol):
            def nothing_at_all(self) -> None: ...

        missing = get_protocol(MissingProtocol)
        assert missing is None

    finally:
        _DYNASTORE_MODULES.clear()
        _DYNASTORE_MODULES.update(original_modules)
