import importlib.metadata
import importlib.metadata
from typing import (
    Dict,
    Type,
    TypeVar,
    Any,
    List,
    cast,
    Optional,
)
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Global plugin registry
# ---------------------------------------------------------------------------
_DYNASTORE_PLUGINS: List[Any] = []

def register_plugin(instance: Any) -> None:
    if instance not in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.append(instance)
        get_protocol.cache_clear()
        get_protocols.cache_clear()

def unregister_plugin(instance: Any) -> None:
    if instance in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.remove(instance)
        get_protocol.cache_clear()
        get_protocols.cache_clear()

@lru_cache(maxsize=128)
def get_protocol(protocol: Type[T]) -> Optional[T]:
    instances = get_protocols(protocol)
    return instances[0] if instances else None

@lru_cache(maxsize=128)
def get_protocols(protocol: Type[T]) -> List[T]:
    discovered: List[T] = []
    seen = set()

    def _accept(obj: Any) -> bool:
        obj_id = id(obj)
        if obj_id in seen:
            return False
        if not isinstance(obj, protocol):
            return False
        if hasattr(obj, "is_available") and not obj.is_available():
            return False
        seen.add(obj_id)
        return True

    # 1. Dynamically registered plugins
    for instance in _DYNASTORE_PLUGINS:
        if _accept(instance):
            discovered.append(cast(T, instance))

    # 2. Modules
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(cast(T, config.instance))

    # 3. Extensions
    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(cast(T, config.instance))
    except (ImportError, ModuleNotFoundError):
        pass

    # 4. Tasks
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(cast(T, config.instance))

    discovered.sort(key=lambda x: getattr(x, "priority", 100))
    return discovered

def discover_and_load_plugins(group: str, include_only: Optional[List[str]] = None) -> Dict[str, Type[Any]]:
    """
    Automatically discovers plugins registered via entry points.
    
    Args:
        group: The entry point group (e.g., 'dynastore.modules').
        include_only: If provided, only entry points with names in this list will be loaded.
                      Normalization (lowercase, _ -> -) is applied for matching.
        
    Returns:
        A dictionary mapping the entry point name to the uninstantiated plugin class.
    """
    loaded_plugins = {}
    
    # 1. Normalize include_only list for robust matching
    target_names: Optional[set[str]] = None
    if include_only is not None:
        target_names = {name.lower().replace("_", "-") for name in include_only}

    # 2. Get all entry points for the specified group
    try:
        eps = importlib.metadata.entry_points(group=group)
    except TypeError:
        # Fallback for older importlib.metadata versions
        eps = importlib.metadata.entry_points().get(group, [])

    for entry_point in eps:
        # 3. Filtering logic
        ep_name_normalized = entry_point.name.lower().replace("_", "-")
        if target_names is not None and ep_name_normalized not in target_names:
            logger.debug(f"Skipping {group}: {entry_point.name} (not in include_only)")
            continue

        try:
            # Load the class/service defined in pyproject.toml
            plugin_class = entry_point.load()
            
            loaded_plugins[entry_point.name] = plugin_class
            logger.info(f"Successfully discovered installed {group}: {entry_point.name}")
            
        except Exception as e:
            logger.error(f"Failed to load installed plugin '{entry_point.name}' from group '{group}': {e}")
            
    return loaded_plugins

def get_installed_module_metadata() -> List[Dict[str, str]]:
    """
    Returns metadata about installed modules for documentation auto-generation.
    Only modules installed via build-time extras will appear here.
    """
    metadata = []
    
    # We check the main groups
    groups = ["dynastore.modules", "dynastore.extensions", "dynastore.tasks"]
    
    for group in groups:
        try:
            eps = importlib.metadata.entry_points(group=group)
        except TypeError:
            eps = importlib.metadata.entry_points().get(group, [])
            
        for ep in eps:
            # Basic metadata derived from the entry point
            item = {
                "name": ep.name,
                "group": group,
                "module": ep.value.split(":")[0],
            }
            
            # Attempt to find the distribution package that provides this entry point
            try:
                if hasattr(ep, "dist") and ep.dist:
                    dist = ep.dist
                    item["package"] = dist.metadata.get("Name", "unknown")
                    item["version"] = dist.version
                    item["summary"] = dist.metadata.get("Summary", "")
            except Exception as e:
                logger.debug(f"Could not extract package metadata for {ep.name}: {e}")
                
            metadata.append(item)
            
    return metadata
