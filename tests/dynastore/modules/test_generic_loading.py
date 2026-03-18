
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
