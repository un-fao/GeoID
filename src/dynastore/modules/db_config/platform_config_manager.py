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

# File: src/dynastore/modules/db_config/platform_config_manager.py

"""
The Platform Config Manager & Registry.
Includes the Immutable field definition and enforcement logic.
"""

import logging
import json
import typing
import asyncio
import inspect
from typing import Any, Dict, Optional, Type, Protocol, runtime_checkable, Generic, TypeVar, Callable, Union, get_origin, get_args, overload, cast, Annotated, List
# Handle UnionType for Python 3.10+
try:
    from types import UnionType
except ImportError:
    UnionType = Union

from pydantic import BaseModel
from async_lru import alru_cache

from dynastore.modules.db_config.query_executor import (
    DQLQuery, DDLQuery, ResultHandler, managed_transaction, DbResource
)
from .maintenance_tools import ensure_schema_exists
#imported to avoid circular imports
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from dynastore.tools.json import CustomJSONEncoder

logger = logging.getLogger(__name__)

# --- Immutability Framework ---

T = TypeVar("T")

class ImmutableMarker:
    """Internal marker for immutability."""
    pass

class Immutable:
    """
    A marker class that supports elegant declaration of immutable fields.
    Usage:
        field: Immutable[int] = Field(...)
    
    This is equivalent to:
        field: Annotated[int, ImmutableMarker] = Field(...)
    """
    def __class_getitem__(cls, item):
        return Annotated[item, ImmutableMarker]

def is_immutable_field(field_info: 'FieldInfo') -> bool:
    """
    Checks if a Pydantic field is annotated as Immutable.
    
    This handles both:
    1. Annotated[T, ImmutableMarker]
    2. Immutable[T] (which resolves to the above)
    3. The legacy Annotated[T, Immutable] (if any still exist)
    """
    # Check 1: Direct annotation (e.g., Annotated[int, ImmutableMarker])
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if ImmutableMarker in args or Immutable in args:
            return True
            
    # Check 2: Pydantic metadata
    if any(item is ImmutableMarker or item is Immutable or (isinstance(item, type) and issubclass(item, ImmutableMarker)) for item in field_info.metadata):
        return True
        
    return False

def enforce_config_immutability(current_config: Optional['PluginConfig'], new_config: 'PluginConfig') -> None:
    """
    Validates that no fields marked as Immutable[T] have changed between current and new config.
    
    Args:
        current_config: The existing configuration (from DB). If None, checks are skipped (creation scenario).
        new_config: The candidate configuration.
    
    Raises:
        ValueError: If an immutable field has been modified.
    """
    if current_config is None:
        return

    # We assume configs are of the same type for comparison
    model_class = type(current_config)
    if not isinstance(new_config, model_class):
        # If types mismatch, we can't reliably compare fields. 
        # In a real scenario, this might be an error or a complete replacement.
        return 

    for field_name, field_info in model_class.model_fields.items():
        # Pass the entire FieldInfo object for robust checking
        if is_immutable_field(field_info):
            current_val = getattr(current_config, field_name)
            new_val = getattr(new_config, field_name)
            
            if current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is Immutable. "
                    f"Modification forbidden: {current_val} -> {new_val}"
                )

# --- Schema (Platform Level Only) ---

PLATFORM_CONFIGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS configs.platform_configs (
    plugin_id VARCHAR NOT NULL PRIMARY KEY,
    config_data JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

# --- Queries ---

get_platform_config_query = DQLQuery(
    "SELECT config_data FROM configs.platform_configs WHERE plugin_id = :plugin_id;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

upsert_platform_config_query = DQLQuery(
    """
    INSERT INTO configs.platform_configs (plugin_id, config_data, updated_at) 
    VALUES (:plugin_id, :config_data, NOW())
    ON CONFLICT (plugin_id) DO UPDATE SET config_data = EXCLUDED.config_data, updated_at = NOW();
    """,
    result_handler=ResultHandler.ROWCOUNT
)

list_platform_configs_query = DQLQuery(
    "SELECT plugin_id, config_data FROM configs.platform_configs;",
    result_handler=ResultHandler.ALL_DICTS
)

delete_platform_config_query = DQLQuery(
    "DELETE FROM configs.platform_configs WHERE plugin_id = :plugin_id;",
    result_handler=ResultHandler.ROWCOUNT
)

# --- Protocols & Models ---

class PluginConfig(BaseModel):
    """
    Base class for all mutable plugin configurations.
    MANDATORY: Subclasses must be instantiable without arguments (all fields must have defaults).
    """
    enabled: bool = True

@runtime_checkable
class HasPlatformConfigManager(Protocol):
    def get_platform_config_manager(self) -> 'PlatformConfigManager': ...

class ConfigRegistry:
    """
    Central registry mapping plugin_ids (e.g. 'stac', 'ingestion') to Pydantic models.
    """
    _registry: Dict[str, Type[PluginConfig]] = {}
    _apply_handlers: Dict[str, Callable[['PluginConfig', Optional[str], Optional[str], Optional[DbResource]], Any]] = {}

    @classmethod
    def register(cls, key: str, model: Type[PluginConfig], on_apply: Optional[Callable[['PluginConfig', Optional[str], Optional[str], Optional[DbResource]], Any]] = None):
        if not issubclass(model, PluginConfig):
            raise ValueError(f"Model {model} must inherit from PluginConfig")
        try:
            model()
        except Exception as e:
            raise ValueError(f"PluginConfig '{key}' ({model.__name__}) must be instantiable without arguments (defaults required). Error: {e}")

        cls._registry[key] = model
        if on_apply:
            cls._apply_handlers[key] = on_apply
        logger.debug(f"Registered configuration schema for '{key}'")

    @classmethod
    def get_apply_handler(cls, key: str) -> Optional[Callable[['PluginConfig', Optional[str], Optional[str], Optional[DbResource]], Any]]:
        return cls._apply_handlers.get(key)

    @classmethod
    def get_model(cls, key: str) -> Optional[Type[PluginConfig]]:
        return cls._registry.get(key)

    @classmethod
    def create_default(cls, key: str) -> PluginConfig:
        model = cls.get_model(key)
        if not model:
            logger.warning(f"Request for unknown config ID: '{key}'. Returning generic PluginConfig.")
            return PluginConfig()
        return model()

    @classmethod
    def validate_config(cls, key: str, config_data: Any) -> PluginConfig:
        model = cls.get_model(key)
        if not model:
            logger.warning(f"Validating unknown config ID: '{key}'.")
            return PluginConfig.model_validate(config_data)

        if isinstance(config_data, model):
            return config_data
        return model.model_validate(config_data)

def register_config(key: str, on_apply: Optional[Callable[['PluginConfig', Optional[str], Optional[str], Optional[DbResource]], Any]] = None):
    def decorator(cls):
        ConfigRegistry.register(key, cls, on_apply=on_apply)
        return cls
    return decorator

# --- Manager ---

class PlatformConfigManager:
    """Manages global configuration settings (Level 3)."""
    
    def __init__(self, engine: DbResource):
        self.engine = engine
        
        # Instance-bound cache for platform configuration
        self.get_platform_config_internal_cached = alru_cache(maxsize=32)(self._get_platform_config_internal_db)

    @classmethod
    async def initialize(cls, conn: DbResource):
        """Initializes the platform configuration storage."""
        try:
            logger.info("Initializing Platform Config Storage (configs schema)...")
            await ensure_schema_exists(conn, "configs")
            await DDLQuery(PLATFORM_CONFIGS_SCHEMA).execute(conn)
            logger.info("Platform Config Storage initialized successfully.")
        except Exception as e:
            logger.error(f"FATAL: PlatformConfigManager initialization failed: {e}", exc_info=True)
            raise

    async def get_config(self, plugin_id: str, db_resource: Optional[DbResource] = None) -> PluginConfig:
        config = await self._get_platform_config_internal(plugin_id, db_resource=db_resource)
        if config: return config
        return ConfigRegistry.create_default(plugin_id)

    async def _get_platform_config_internal_db(self, plugin_id: str) -> Optional[dict]:
        """Internal fetcher (returned as dict for immutability)."""
        async with managed_transaction(self.engine) as conn:
            return await get_platform_config_query.execute(conn, plugin_id=plugin_id)

    async def _get_platform_config_internal(self, plugin_id: str, db_resource: Optional[DbResource] = None) -> Optional[PluginConfig]:
        """Internal fetcher that respects the provided db_resource, falling back to cache."""
        if db_resource:
            # Execute directly on the provided connection - it's already in a transaction
            data = await get_platform_config_query.execute(db_resource, plugin_id=plugin_id)
        else:
            # Fall back to cached version which creates its own transaction
            data = await self.get_platform_config_internal_cached(plugin_id)
            
        return ConfigRegistry.validate_config(plugin_id, data) if data else None

    async def set_config(
        self, 
        plugin_id: str, 
        config: PluginConfig, 
        validator: Optional[Callable[[Optional[PluginConfig], PluginConfig], None]] = None
    ) -> None:
        """
        Writes configuration to the Platform level.
        
        Args:
            validator: Optional callback(current, new) to enforce logic (e.g. immutability).
        """
        async with managed_transaction(self.engine) as conn:
            if validator:
                current_data = await get_platform_config_query.execute(conn, plugin_id=plugin_id)
                current_config = ConfigRegistry.validate_config(plugin_id, current_data) if current_data else None
                validator(current_config, config)

            await upsert_platform_config_query.execute(
                conn, plugin_id=plugin_id, config_data=json.dumps(config.model_dump(), cls=CustomJSONEncoder)
            )

            # Trigger active configuration application (Level 3 - Platform)
            apply_handler = ConfigRegistry.get_apply_handler(plugin_id)
            if apply_handler:
                try:
                    res = apply_handler(config, None, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(f"Failed to apply platform configuration for '{plugin_id}': {e}", exc_info=True)

        self.get_platform_config_internal_cached.cache_invalidate(plugin_id)

    async def list_configs(self) -> Dict[str, PluginConfig]:
        """Lists all configurations set at the platform level."""
        async with managed_transaction(self.engine) as conn:
            rows = await list_platform_configs_query.execute(conn)
        
        configs = {}
        for row in rows:
            plugin_id = row['plugin_id']
            config_data = row['config_data']
            configs[plugin_id] = ConfigRegistry.validate_config(plugin_id, config_data)
        return configs

    async def delete_config(self, plugin_id: str) -> bool:
        """Deletes a configuration at the platform level."""
        async with managed_transaction(self.engine) as conn:
            rows_affected = await delete_platform_config_query.execute(conn, plugin_id=plugin_id)
            if rows_affected > 0:
                self.get_platform_config_internal_cached.cache_invalidate(plugin_id)
                return True
        return False