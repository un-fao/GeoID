"""Architectural invariant: no PluginConfig subclass may declare `_plugin_id`.

Identity is the Pydantic class itself (via `class_key()` == `__qualname__`).
Legacy string IDs are gone. This test prevents regression.
"""
from importlib.metadata import entry_points


def _load_all_modules() -> None:
    for ep in entry_points(group="dynastore.modules"):
        try:
            ep.load()
        except Exception:
            pass


def test_no_plugin_id_classvar():
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.platform_config_service import PluginConfig

    offenders = [
        cls.__qualname__
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if "_plugin_id" in cls.__dict__
    ]
    assert not offenders, (
        f"PluginConfig subclasses must not declare `_plugin_id`; identity is the class itself. "
        f"Offenders: {offenders}"
    )


def test_no_legacy_class_keys():
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.platform_config_service import PluginConfig

    bad = [
        cls.class_key()
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if ":" in cls.class_key() or cls.class_key().islower()
    ]
    assert not bad, f"class_key must be PascalCase __qualname__; legacy keys found: {bad}"
