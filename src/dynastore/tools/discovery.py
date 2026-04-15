import importlib.metadata
import re
from typing import (
    Dict,
    Set,
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

_NORMALIZE_RE = re.compile(r"[-_.]+")


def _normalize(name: str) -> str:
    """Normalize a Python package/extras/entry-point name for comparison.

    PEP 685 specifies that runs of ``[-_.]`` are equivalent and should be
    collapsed to a single hyphen for canonical comparison.  We apply the same
    rule so that ``extension_catalog_events``, ``extension-catalog-events``,
    and ``extension.catalog.events`` all match each other.
    """
    return _NORMALIZE_RE.sub("-", name).lower()


# ---------------------------------------------------------------------------
# Scope resolution: pip extras name → entry-point names
# ---------------------------------------------------------------------------

def _build_extras_graph() -> Dict[str, Set[str]]:
    """
    Build a directed graph of extras from all packages that contribute to
    dynastore entry-point groups (dynastore.modules, dynastore.extensions,
    dynastore.tasks).

    This allows downstream packages to define their own self-referencing
    extras that resolve through the same SCOPE mechanism::

        # my-project pyproject.toml
        [project.optional-dependencies]
        module_foo = []
        extension_bar = []
        my_app = ["my-project[module_foo,extension_bar]"]

    With this, ``SCOPE=my_app`` resolves to ``foo``, ``bar``, etc.
    Names are compared using PEP 685 normalization (``_`` ≡ ``-``).
    """
    # Discover all packages contributing to dynastore entry-point groups
    package_names: Set[str] = {"DynaStore"}
    for group in ("dynastore.modules", "dynastore.extensions", "dynastore.tasks"):
        try:
            eps = importlib.metadata.entry_points(group=group)
        except TypeError:
            eps = importlib.metadata.entry_points().get(group, [])
        for ep in eps:
            if hasattr(ep, "dist") and ep.dist:
                package_names.add(ep.dist.metadata["Name"])

    graph: Dict[str, Set[str]] = {}
    extra_re = re.compile(r'extra\s*==\s*"([^"]+)"')

    for pkg_name in package_names:
        try:
            requires = importlib.metadata.requires(pkg_name) or []
        except importlib.metadata.PackageNotFoundError:
            continue

        # Match self-referencing deps like  pkg[extra]
        self_ref_re = re.compile(
            rf"^{re.escape(pkg_name)}\[([^\]]+)\]", re.IGNORECASE
        )

        for line in requires:
            m_extra = extra_re.search(line)
            if not m_extra:
                continue
            parent = _normalize(m_extra.group(1))
            graph.setdefault(parent, set())

            m_ref = self_ref_re.match(line.strip())
            if m_ref:
                for child in m_ref.group(1).split(","):
                    child = _normalize(child.strip())
                    if child:
                        graph[parent].add(child)

    return graph


def resolve_scope(scope_csv: str) -> Set[str]:
    """
    Resolve a comma-separated SCOPE string (e.g. ``"api_catalog"``) into the
    full set of names that should be matched against entry-point names.

    The resolution works in two steps:

    1. **Transitive closure** – walk the extras dependency graph so that
       ``api_catalog`` expands to every extras it (transitively) pulls in.
    2. **Prefix stripping** – for every resolved extras whose name starts with
       ``module-``, ``extension-``, or ``task-``, add the base name as well
       (e.g. ``extension-admin`` → ``admin``).  This ensures the result can be
       matched against entry-point names which omit these prefixes.
    """
    graph = _build_extras_graph()

    seeds = {_normalize(s.strip()) for s in scope_csv.split(",")}
    resolved: Set[str] = set()
    stack = list(seeds)

    while stack:
        name = stack.pop()
        if name in resolved:
            continue
        resolved.add(name)
        for child in graph.get(name, set()):
            if child not in resolved:
                stack.append(child)

    # Expand: strip known prefixes so that entry-point names are included.
    expanded: Set[str] = set()
    for name in resolved:
        expanded.add(name)
        for prefix in ("module-", "extension-", "task-"):
            if name.startswith(prefix):
                expanded.add(name[len(prefix):])

    logger.info(f"Scope '{scope_csv}' resolved to entry-point names: {sorted(expanded)}")
    return expanded

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Global plugin registry
# ---------------------------------------------------------------------------
_DYNASTORE_PLUGINS: List[Any] = []

def register_plugin(instance: Any) -> None:
    if instance not in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.append(instance)
        _get_protocol_cached.cache_clear()
        _get_protocols_cached.cache_clear()

def unregister_plugin(instance: Any) -> None:
    if instance in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.remove(instance)
        _get_protocol_cached.cache_clear()
        _get_protocols_cached.cache_clear()

@lru_cache(maxsize=128)
def _get_protocol_cached(protocol: type) -> Optional[Any]:
    instances = _get_protocols_cached(protocol)
    return instances[0] if instances else None

@lru_cache(maxsize=128)
def _get_protocols_cached(protocol: type) -> List[Any]:
    discovered: List[Any] = []
    seen: set = set()

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
            discovered.append(instance)

    # 2. Modules
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    # 3. Extensions
    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(config.instance)
    except (ImportError, ModuleNotFoundError):
        pass

    # 4. Tasks
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    discovered.sort(key=lambda x: getattr(x, "priority", 100))
    return discovered


def get_protocol(protocol: Type[T]) -> Optional[T]:
    """Return the first registered instance of the given protocol, or None."""
    return cast(Optional[T], _get_protocol_cached(protocol))


def get_protocols(protocol: Type[T]) -> List[T]:
    """Return all registered instances of the given protocol, sorted by priority."""
    return cast(List[T], _get_protocols_cached(protocol))


def get_all_protocols(protocol: Type[T]) -> List[T]:
    """Return all registered instances of the given protocol — including
    those whose ``is_available()`` currently returns False. Intended for
    introspection UIs (e.g. driver listings for a configurator)."""
    discovered: List[Any] = []
    seen: set = set()

    def _accept(obj: Any) -> bool:
        obj_id = id(obj)
        if obj_id in seen or not isinstance(obj, protocol):
            return False
        seen.add(obj_id)
        return True

    for instance in _DYNASTORE_PLUGINS:
        if _accept(instance):
            discovered.append(instance)

    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(config.instance)
    except (ImportError, ModuleNotFoundError):
        pass

    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    discovered.sort(key=lambda x: getattr(x, "priority", 100))
    return cast(List[T], discovered)


def _clear_protocol_caches() -> None:
    _get_protocol_cached.cache_clear()
    _get_protocols_cached.cache_clear()


# Expose cache_clear on the wrapper functions so callers (e.g. tests) can
# invalidate the caches via `get_protocol.cache_clear()`.
get_protocol.cache_clear = _clear_protocol_caches  # type: ignore[attr-defined]
get_protocols.cache_clear = _clear_protocol_caches  # type: ignore[attr-defined]

def discover_and_load_plugins(group: str, include_only: Optional[List[str]] = None) -> Dict[str, Type[Any]]:
    """
    Automatically discovers plugins registered via entry points.

    Args:
        group: The entry point group (e.g., 'dynastore.modules').
        include_only: If provided, only entry points with names in this list will be loaded.
                      Names can be direct entry-point names **or** pip extras names
                      (e.g. ``"api_catalog"``).  Extras names are recursively resolved
                      to the full set of constituent entry-point names.
                      PEP 685 normalization is applied for matching.

    Returns:
        A dictionary mapping the entry point name to the uninstantiated plugin class.
    """
    loaded_plugins = {}

    # 1. Resolve include_only through the extras dependency graph so that
    #    high-level scope names like "api_catalog" expand to concrete
    #    entry-point names like "catalog", "web", "db", etc.
    target_names: Optional[Set[str]] = None
    if include_only is not None:
        target_names = resolve_scope(",".join(include_only))

    # 2. Get all entry points for the specified group
    try:
        eps = importlib.metadata.entry_points(group=group)
    except TypeError:
        # Fallback for older importlib.metadata versions
        eps = importlib.metadata.entry_points().get(group, [])

    for entry_point in eps:
        # 3. Filtering logic
        ep_name_normalized = _normalize(entry_point.name)
        if target_names is not None and ep_name_normalized not in target_names:
            logger.debug(f"Skipping {group}: {entry_point.name} (not in include_only)")
            continue

        try:
            # Load the class/service defined in pyproject.toml
            plugin_class = entry_point.load()

            loaded_plugins[entry_point.name] = plugin_class
            logger.info(f"Successfully discovered installed {group}: {entry_point.name}")

        except ImportError as e:
            logger.info(
                f"Skipping {group} plugin '{entry_point.name}': "
                f"optional dependencies not installed — {e}"
            )
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
