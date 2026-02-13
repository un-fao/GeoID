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

"""
Protocol Migration and Resolution Helpers

This module provides standardized, professional access to platform protocols.
It replaces legacy direct module access patterns and the deprecated get_any_engine().

Access Strategies:
1. resolve(ProtocolClass): For one-time resolution of a specific protocol.
2. get_engine(): Specialized accessor for the most frequent dependency.
"""

from typing import Type, TypeVar, Optional, Any
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.database import DatabaseProtocol

T = TypeVar("T")

def resolve(protocol_type: Type[T]) -> T:
    """
    Standardized, fail-fast resolution of a protocol.
    
    Args:
        protocol_type: The protocol class to resolve
        
    Returns:
        The registered implementation of the protocol
        
    Raises:
        RuntimeError: If no implementation is found for the protocol
    """
    instance = get_protocol(protocol_type)
    if not instance:
        # Professionals prefer clear, actionable error messages
        raise RuntimeError(
            f"Required protocol '{protocol_type.__name__}' is not available. "
            "Ensure the responsible module is listed in DYNASTORE_MODULES "
            "and has been correctly initialized."
        )
    return instance


def get_engine(app_state: Optional[Any] = None) -> Any:
    """
    Standardized database engine access.
    
    This replaces the legacy get_any_engine() pattern. It transparently
    resolves the highest priority DatabaseProtocol and returns its engine.
    
    Args:
        app_state: Optional app_state for backward compatibility (ignored)
    
    Returns:
        Database engine instance
    """
    db = get_protocol(DatabaseProtocol)
    if not db:
        raise RuntimeError("DatabaseProtocol is not available. Check database module initialization.")
    return db.engine
