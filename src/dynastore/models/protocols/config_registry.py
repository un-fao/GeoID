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

"""
Protocol for the ConfigRegistry -- discoverable via ``get_protocol(ConfigRegistryProtocol)``.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Protocol, Type, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class ConfigRegistryProtocol(Protocol):
    """Central configuration registry protocol.

    Discoverable via ``get_protocol(ConfigRegistryProtocol)``.
    Manages mapping between plugin IDs and their Pydantic config models.
    """

    def get_model(self, key: str) -> Optional[Type[BaseModel]]:
        """Get the Pydantic model class for a config key."""
        ...

    def create_default(self, key: str) -> BaseModel:
        """Create a default instance of the config for a key."""
        ...

    def validate_config(self, key: str, config_data: Any) -> BaseModel:
        """Validate raw data against the registered model."""
        ...

    def register(
        self,
        key: str,
        model: Type[BaseModel],
        on_apply: Optional[Callable] = None,
    ) -> None:
        """Register a config model for a key."""
        ...

    def register_apply_handler(self, key: str, handler: Callable) -> None:
        """Register an additional on_apply handler for a config key."""
        ...

    def get_apply_handlers(self, key: str) -> List[Callable]:
        """Get all registered apply handlers for a config key."""
        ...

    def list_registered(self) -> Dict[str, Type[BaseModel]]:
        """List all registered config models."""
        ...
