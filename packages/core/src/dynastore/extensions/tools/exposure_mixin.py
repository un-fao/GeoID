"""Shared mixin and registry constants for the service-exposure control panel."""

from pydantic import BaseModel, Field

from dynastore.models.mutability import Mutable


class ExposableConfigMixin(BaseModel):
    """Adds a per-scope `enabled` toggle to any PluginConfig."""

    enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When False, the extension is unavailable at this scope and returns "
            "503 Service Unavailable. At platform scope, the routes are also "
            "omitted from the OpenAPI schema."
        ),
    )


# Legacy fallback constants.  These are kept for backward compatibility and
# as a safety net for extensions that have not yet adopted the declarative
# ``always_on = True`` class attribute (see ``ExtensionProtocol``).
# "tools" and "documentation" are internal core names that have no registered
# extension entry-point; they appear here only to suppress false-positive
# reports from ``find_dead_exposable_configs``.
ALWAYS_ON_EXTENSIONS: frozenset[str] = frozenset({
    "iam", "auth", "configs", "web", "admin",
    "tools", "template", "httpx", "documentation",
})

KNOWN_EXTENSION_IDS: frozenset[str] = frozenset({
    *ALWAYS_ON_EXTENSIONS,
    "stac", "features", "wfs", "coverages", "edr", "records", "processes", "dggs",
    "tiles", "maps", "styles", "dimensions", "dwh", "joins", "search", "stats",
    "gcp", "logs", "notebooks", "crs", "gdal", "assets", "moving_features",
    "connected_systems", "volumes",
})


def _get_dynamic_sets() -> tuple[frozenset[str], frozenset[str]]:
    """Return (always_on, known) derived from the live extension registry.

    Falls back to the hardcoded constants when the registry is not yet
    populated (e.g. during config-file import before bootstrap has run).
    The dynamic sets are preferred because they stay consistent with whatever
    extensions are actually installed — no manual list maintenance required.
    """
    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        if not _DYNASTORE_EXTENSIONS:
            return ALWAYS_ON_EXTENSIONS, KNOWN_EXTENSION_IDS
        known = frozenset(_DYNASTORE_EXTENSIONS.keys())
        always_on = frozenset(
            name for name, cfg in _DYNASTORE_EXTENSIONS.items()
            if getattr(cfg.cls, "always_on", False)
        ) | frozenset(n for n in ALWAYS_ON_EXTENSIONS if n not in known)
        return always_on, known | ALWAYS_ON_EXTENSIONS
    except Exception:
        return ALWAYS_ON_EXTENSIONS, KNOWN_EXTENSION_IDS


def find_dead_exposable_configs() -> list[tuple[type, str]]:
    """Return registered ``ExposableConfigMixin`` subclasses that
    ``ExposureMatrix`` cannot enforce.

    The matrix in ``extensions/lifespan.py`` only consults ``enabled`` for
    configs whose module path matches ``dynastore.{extensions,modules}.<ext>``
    AND the ext is togglable (i.e. known but not always-on). Any other use
    of the mixin produces an ``enabled`` field that appears in
    ``GET /configs/...`` responses but flips without effect — the operator
    bug #853/#854 cleaned up.

    Also reports **fan-out shadowing** (#855 item 1): when multiple
    ``ExposableConfigMixin`` subclasses share the same ``parts[2]`` ext
    name, ``ExposureMatrix`` only consults the first-registered (via
    ``plugin_cls_by_ext.setdefault``); the siblings' toggles silently
    no-op. The fix is to demote sibling configs to plain ``PluginConfig``
    and (if they need their own boolean knob) declare it under a distinct
    field name — see ``GcpCatalogBucketConfig.provision_enabled`` and
    ``TilesCachingConfig.cache_enabled`` for canonical examples.
    """
    # Local import to avoid a registration-time cycle: this module is
    # imported by config files before the platform service is wired.
    from dynastore.modules.db_config.plugin_config import list_registered_configs

    always_on, known = _get_dynamic_sets()
    togglable = known - always_on
    dead: list[tuple[type, str]] = []
    per_ext: dict[str, list[type]] = {}
    for cls in list_registered_configs().values():
        if not issubclass(cls, ExposableConfigMixin):
            continue
        parts = cls.__module__.split(".")
        if not (len(parts) >= 3 and parts[1] in ("extensions", "modules")):
            dead.append((
                cls,
                f"module path {cls.__module__!r} is not under "
                "dynastore.{extensions,modules}.<ext>",
            ))
            continue
        ext = parts[2]
        if ext in always_on:
            dead.append((
                cls,
                f"extension {ext!r} is always-on — routes are mounted "
                "unconditionally, `enabled` is never consulted",
            ))
            continue
        if ext not in togglable:
            dead.append((
                cls,
                f"extension {ext!r} is not in the known extension registry — "
                "ExposureMatrix skips it; register the extension or declare "
                "``always_on = True`` if it should never be gated",
            ))
            continue
        per_ext.setdefault(ext, []).append(cls)

    for ext, classes in per_ext.items():
        if len(classes) < 2:
            continue
        siblings = ", ".join(sorted(c.__name__ for c in classes))
        for cls in classes:
            dead.append((
                cls,
                f"extension {ext!r} has {len(classes)} ExposableConfigMixin "
                f"subclasses ({siblings}); ExposureMatrix consults only the "
                "first-registered via setdefault — strip the mixin from the "
                "non-canonical siblings (and declare any local boolean knob "
                "under a distinct field name)",
            ))
    return dead
