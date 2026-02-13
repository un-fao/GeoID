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

import logging
import inspect
from functools import lru_cache
from typing import Type, TypeVar, List, Optional, Any, Callable, Dict, Protocol, runtime_checkable, Tuple, Set, cast
from contextlib import AsyncExitStack

T = TypeVar("T")

logger = logging.getLogger(__name__)

# Registry for dynamically registered protocol providers
# This allows managers and services to be registered as protocol implementers
# without being full Modules/Extensions
_DYNASTORE_PROVIDERS: List[object] = []


class Provider:
    """
    Descriptor for declarative provider registration.
    
    Usage:
        class MyModule:
            my_service = Provider(MyService, priority=10)
            
    The descriptor will:
    1. Instantiate the provider class lazily on first access
    2. Automatically register it with the discovery system
    3. Call initialize(app_state, **deps) if the method exists
    """
    
    def __init__(
        self,
        provider_class: Type,
        priority: int = 0,
        factory: Optional[Callable[..., Any]] = None
    ):
        """
        Args:
            provider_class: The class to instantiate as a provider
            priority: Priority for protocol discovery (higher = preferred)
            factory: Optional factory function to create the instance
        """
        self.provider_class = provider_class
        self.priority = priority
        self.factory = factory
        self.instance = None
        self.attr_name = None
    
    def __set_name__(self, owner, name):
        """Called when the descriptor is assigned to a class attribute."""
        self.attr_name = name
    
    def __get__(self, obj, objtype=None):
        """Lazy instantiation and registration on first access."""
        if obj is None:
            return self
        
        # Check instance dict first (per-module-instance cache)
        if self.attr_name and self.attr_name in obj.__dict__:
            return obj.__dict__[self.attr_name]
        
        # Create instance using factory or default constructor
        if self.factory:
            instance = self.factory()
        else:
            instance = self.provider_class()
        
        # Set priority if not already set
        if not hasattr(instance, 'priority'):
            instance.priority = self.priority
        
        # Register with discovery system
        register_provider(instance)
        
        logger.debug(f"Provider {self.provider_class.__name__} instantiated and registered (priority={self.priority})")
        
        # Cache on the module instance
        if self.attr_name:
            obj.__dict__[self.attr_name] = instance
        
        return instance


async def initialize_providers(
    module_instance: object,
    app_state: Any,
    db_resource: Optional[Any] = None,
    stack: Optional[AsyncExitStack] = None
) -> List[object]:
    """
    Discovers and initializes Provider descriptors on a module instance.
    Supports both 'lifespan' (context manager) and legacy 'initialize' (coroutine) methods.
    
    Args:
        module_instance: The module instance containing Provider descriptors
        app_state: Application state to pass to initialize()/lifespan()
        db_resource: Optional database connection to pass
        stack: Optional AsyncExitStack to register lifespan exit callbacks
        
    Returns:
        List of initialized provider instances
    """
    providers = []
    
    # Discover all Provider descriptors
    for attr_name in dir(module_instance.__class__):
        attr = getattr(module_instance.__class__, attr_name)
        if isinstance(attr, Provider):
            # Access the descriptor to trigger lazy instantiation
            instance = getattr(module_instance, attr_name)
            providers.append((attr.priority, attr_name, instance))
    
    # Sort by priority (descending)
    providers.sort(key=lambda x: x[0], reverse=True)
    
    # Initialize each provider
    initialized = []

    for priority, name, instance in providers:
        try:
            # Check for 'lifespan' (Async Context Manager)
            if hasattr(instance, 'lifespan'):
                if stack:
                    # Determine args for lifespan
                    sig = inspect.signature(instance.lifespan)
                    kwargs = {}
                    if 'db_resource' in sig.parameters:
                        kwargs['db_resource'] = db_resource
                    
                    cm = instance.lifespan(app_state, **kwargs)
                    await stack.enter_async_context(cm)
                    logger.info(f"Entered lifespan for provider {name} (priority={priority})")
                else:
                    logger.warning(f"Provider {name} has lifespan but no stack provided. Initialization skipped to avoid unclosed resources.")
                    continue

            # Fallback to 'initialize' (Coroutine)
            elif hasattr(instance, 'initialize'):
                # Call initialize with app_state
                sig = inspect.signature(instance.initialize)
                kwargs = {}
                if 'db_resource' in sig.parameters:
                    kwargs['db_resource'] = db_resource
                await instance.initialize(app_state, **kwargs)
                logger.info(f"Initialized provider {name} (priority={priority})")
            
            initialized.append(instance)
            
        except Exception as e:
            logger.error(f"Failed to initialize provider {name}: {e}", exc_info=True)
            raise
    
    if not initialized:
        logger.warning(f"No providers initialized for {module_instance.__class__.__name__}")
    
    return initialized


def register_provider(instance: object) -> None:
    """
    Registers an arbitrary object as a protocol provider.
    
    This enables managers and services to be discovered via get_protocol()
    without requiring them to be full Modules or Extensions.
    
    Args:
        instance: Any object implementing one or more protocols.
    """
    if instance not in _DYNASTORE_PROVIDERS:
        _DYNASTORE_PROVIDERS.append(instance)
        # Clear cache when new providers are registered
        get_protocol.cache_clear()
        get_protocols.cache_clear()

def unregister_provider(instance: object) -> None:
    """
    Unregisters a protocol provider.
    
    Args:
        instance: The provider instance to remove.
    """
    if instance in _DYNASTORE_PROVIDERS:
        _DYNASTORE_PROVIDERS.remove(instance)
        # Clear cache when providers are unregistered
        get_protocol.cache_clear()
        get_protocols.cache_clear()

@lru_cache(maxsize=128)
def get_protocol(protocol: Type[T]) -> Optional[T]:
    """
    Unified protocol discovery across instantiated Modules, Extensions, Tasks, and Providers.
    Returns the highest priority available instance that implements the protocol.
    
    Args:
        protocol: A @runtime_checkable Protocol class.
        
    Returns:
        The highest priority discovered instance that implements the Protocol, or None.
    """
    instances = get_protocols(protocol)
    return instances[0] if instances else None

@lru_cache(maxsize=128)
def get_protocols(protocol: Type[T]) -> List[T]:
    """
    Returns all discovered instances that implement the Protocol, sorted by priority (descending).
    Only returns instances where is_available() returns True.
    """
    discovered: List[T] = []
    
    # 1. Search Dynamically Registered Providers (highest priority)
    for instance in _DYNASTORE_PROVIDERS:
        if isinstance(instance, protocol):
            if not hasattr(instance, 'is_available') or instance.is_available():
                discovered.append(cast(T, instance))
    
    # 2. Search Modules
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and isinstance(config.instance, protocol):
            if not hasattr(config.instance, 'is_available') or config.instance.is_available():
                discovered.append(cast(T, config.instance))
            
    # 3. Search Extensions
    from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
    for config in _DYNASTORE_EXTENSIONS.values():
        if config.instance and isinstance(config.instance, protocol):
            if not hasattr(config.instance, 'is_available') or config.instance.is_available():
                discovered.append(cast(T, config.instance))
            
    # 4. Search Tasks
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and isinstance(config.instance, protocol):
            if not hasattr(config.instance, 'is_available') or config.instance.is_available():
                discovered.append(cast(T, config.instance))
            
    # Sort by priority (default to 0 if not present)
    discovered.sort(key=lambda x: getattr(x, 'priority', 0), reverse=True)
    
    return discovered
