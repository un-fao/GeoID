import pytest
import logging
from typing import Protocol, runtime_checkable
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.modules import _DYNASTORE_MODULES, ModuleConfig
from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS, ExtensionConfig
from dynastore.tasks import _DYNASTORE_TASKS, TaskConfig

@runtime_checkable
class MockProtocol(Protocol):
    def echo(self, msg: str) -> str:
        ...

class HighPriorityImpl:
    priority = 10
    def is_available(self): return True
    def echo(self, msg): return f"High: {msg}"

class LowPriorityImpl:
    priority = 5
    def is_available(self): return True
    def echo(self, msg): return f"Low: {msg}"

class UnavailableImpl:
    priority = 20
    def is_available(self): return False
    def echo(self, msg): return f"Unavailable: {msg}"

def test_priority_discovery():
    # Clear cache for the test
    get_protocol.cache_clear()
    get_protocols.cache_clear()
    
    # Setup mock registry state
    # We'll temporarily inject into the private registries
    orig_modules = _DYNASTORE_MODULES.copy()
    _DYNASTORE_MODULES.clear()
    
    try:
        high_inst = HighPriorityImpl()
        low_inst = LowPriorityImpl()
        unavail_inst = UnavailableImpl()
        
        _DYNASTORE_MODULES["high"] = ModuleConfig(cls=HighPriorityImpl, instance=high_inst)
        _DYNASTORE_MODULES["low"] = ModuleConfig(cls=LowPriorityImpl, instance=low_inst)
        _DYNASTORE_MODULES["unavail"] = ModuleConfig(cls=UnavailableImpl, instance=unavail_inst)
        
        # 1. Test get_protocol returns the highest priority AVAILABLE instance
        best = get_protocol(MockProtocol)
        assert best is not None
        assert best.echo("hello") == "High: hello"
        
        # 2. Test get_protocols returns all available instances sorted by priority
        all_insts = get_protocols(MockProtocol)
        assert len(all_insts) == 2
        assert all_insts[0] == high_inst
        assert all_insts[1] == low_inst
        
        # 3. Test fallback when high priority becomes unavailable
        get_protocol.cache_clear()
        get_protocols.cache_clear()
        
        high_inst.is_available = lambda: False
        
        best_fallback = get_protocol(MockProtocol)
        assert best_fallback is not None
        assert best_fallback.echo("hello") == "Low: hello"
        
    finally:
        # Restore original state
        _DYNASTORE_MODULES.clear()
        _DYNASTORE_MODULES.update(orig_modules)

if __name__ == "__main__":
    pytest.main([__file__])
