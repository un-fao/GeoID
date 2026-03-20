# Configuration Framework

## Overview

The platform manages plugin configurations at multiple levels:

1. **Platform level** — global settings stored in `configs.platform_configs`
2. **Catalog level** — per-catalog overrides stored in catalog schemas
3. **Collection level** — per-collection overrides stored in catalog schemas

Configurations are Pydantic models inheriting from `PluginConfig`. Storage is abstracted behind the `PlatformConfigsProtocol` and `ConfigsProtocol` protocols.

## Registering a Configuration

Configs are auto-discovered via `PluginConfig.__init_subclass__`. Declare `_plugin_id` as a `ClassVar`:

```python
from typing import ClassVar, Optional, Callable
from dynastore.modules.db_config.platform_config_service import PluginConfig, Immutable

SECURITY_PLUGIN_CONFIG_ID = "security"

class SecurityPluginConfig(PluginConfig):
    _plugin_id: ClassVar[Optional[str]] = SECURITY_PLUGIN_CONFIG_ID
    _on_apply: ClassVar[Optional[Callable]] = on_security_config_changed  # optional
    _priority: ClassVar[int] = 100  # optional, default 100

    max_login_attempts: int = 5
    lockout_duration_seconds: int = 300
    api_key_prefix: Immutable[str] = "dyn_"  # cannot be changed after creation
```

When the class is defined, `__init_subclass__` automatically calls `ConfigRegistry.register()`.

**Requirements:**
- All fields must have defaults (instantiable without arguments)
- `_plugin_id` triggers auto-registration
- `_on_apply` callback is called when config is written

## Immutable Fields

Fields marked with `Immutable[T]` cannot be changed after initial creation:

```python
class MyConfig(PluginConfig):
    _plugin_id: ClassVar[Optional[str]] = "my_plugin"

    name: Immutable[str] = "default"  # locked after first write
    description: str = ""              # can be changed anytime
```

Attempting to modify an immutable field raises `ImmutableConfigError`.

## Change Reactions

When a configuration is written, `set_config()` computes a field-level diff and notifies handlers:

```python
@dataclass
class ConfigChange:
    plugin_id: str
    old_config: Optional[PluginConfig]
    new_config: PluginConfig
    changed_fields: Dict[str, Tuple[Any, Any]]  # field -> (old_val, new_val)
    catalog_id: Optional[str] = None
    collection_id: Optional[str] = None
    db_resource: Optional[Any] = None

    @property
    def is_creation(self) -> bool:
        return self.old_config is None

    def field_changed(self, name: str) -> bool:
        return name in self.changed_fields
```

### Registering Apply Handlers

Multiple handlers can be registered for the same config key:

```python
from dynastore.modules.db_config.platform_config_service import ConfigRegistry

# Via _on_apply ClassVar (auto-registered with the config)
class MyConfig(PluginConfig):
    _plugin_id: ClassVar[Optional[str]] = "my_plugin"
    _on_apply: ClassVar[Optional[Callable]] = my_handler

# Or manually at runtime
ConfigRegistry.register_apply_handler("security", on_security_config_changed)
```

Handlers are called with signature: `handler(config, catalog_id, collection_id, db_resource)`.

## ConfigRegistry

The `ConfigRegistry` is the central registry mapping plugin IDs to Pydantic config models. It is discoverable via protocol:

```python
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.config_registry import ConfigRegistryProtocol

registry = get_protocol(ConfigRegistryProtocol)
model = registry.get_model("security")        # -> SecurityPluginConfig class
default = registry.create_default("security")  # -> SecurityPluginConfig()
all_configs = registry.list_registered()        # -> Dict[str, Type[PluginConfig]]
```

## CORS Middleware (Push-Based)

The `DynamicCORSMiddleware` rebuilds its Starlette `CORSMiddleware` inner layer on push from the config framework — no polling:

1. On startup: `initialize_from_db()` reads current config
2. On config write: `on_security_config_changed()` callback triggers `_rebuild()`
3. Request handling: pure pass-through to inner middleware

The handler is registered in the web extension lifespan:
```python
ConfigRegistry.register_apply_handler(SECURITY_PLUGIN_CONFIG_ID, on_security_config_changed)
```
