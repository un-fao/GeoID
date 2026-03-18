import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath("src"))

from dynastore.modules.protocols import ModuleProtocol
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tools.plugin import ProtocolPlugin

def check_protocol(name, proto):
    print(f"Checking {name}...")
    try:
        class Dummy: pass
        issubclass(Dummy, proto)
        print(f"  {name} is OK for issubclass")
    except TypeError as e:
        print(f"  {name} FAILED: {e}")

if __name__ == "__main__":
    check_protocol("ProtocolPlugin", ProtocolPlugin)
    check_protocol("ModuleProtocol", ModuleProtocol)
    check_protocol("TaskProtocol", TaskProtocol)
