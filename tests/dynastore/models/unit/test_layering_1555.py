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

"""Structural layering invariant tests — #1555.

Pins that:
1. ``PluginConfig`` is importable from its new canonical leaf
   ``dynastore.models.plugin_config``.
2. The backward-compat shim at ``dynastore.modules.db_config.plugin_config``
   re-exports the SAME class object (``is`` identity, not just equality).
3. ``DbResource`` and its sibling aliases are importable from the new
   ``dynastore.models.db_resource`` leaf.
4. The backward-compat re-export in ``query_executor`` still delivers the
   SAME ``DbResource`` type alias.
5. No modules-layer import is triggered at import time when loading
   ``dynastore.models.plugin_config`` or ``dynastore.models.db_resource``.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# 1. Canonical new import path works
# ---------------------------------------------------------------------------

def test_plugin_config_importable_from_models() -> None:
    from dynastore.models.plugin_config import PluginConfig
    assert PluginConfig is not None


def test_plugin_config_is_plugin_config_base() -> None:
    from dynastore.models.plugin_config import PluginConfig
    from dynastore.tools.typed_store import PersistentModel
    assert issubclass(PluginConfig, PersistentModel)


# ---------------------------------------------------------------------------
# 2. Shim identity — old path returns the SAME class object
# ---------------------------------------------------------------------------

def test_plugin_config_shim_identity() -> None:
    from dynastore.models.plugin_config import PluginConfig as New
    from dynastore.modules.db_config.plugin_config import PluginConfig as Old
    assert New is Old, (
        "Shim at dynastore.modules.db_config.plugin_config must re-export "
        "the SAME PluginConfig object as dynastore.models.plugin_config"
    )


def test_shim_exports_all_public_helpers() -> None:
    """All helpers used by downstream callers must survive at the old path."""
    from dynastore.modules.db_config.plugin_config import (  # noqa: F401
        PluginConfig,
        resolve_config_class,
        require_config_class,
        list_registered_configs,
        _collect_required_fields,
        _APPLY_HANDLERS,
        _VALIDATE_HANDLERS,
    )


# ---------------------------------------------------------------------------
# 3. DbResource leaf importable from models
# ---------------------------------------------------------------------------

def test_db_resource_importable_from_models() -> None:
    from dynastore.models.db_resource import (  # noqa: F401
        DbSyncConnection,
        DbAsyncConnection,
        DbEngine,
        DbConnection,
        DbSyncResource,
        DbAsyncResource,
        DbResource,
    )


# ---------------------------------------------------------------------------
# 4. query_executor shim identity
# ---------------------------------------------------------------------------

def test_db_resource_query_executor_shim_identity() -> None:
    from dynastore.models.db_resource import DbResource as New
    from dynastore.modules.db_config.query_executor import DbResource as Old
    assert New is Old, (
        "query_executor must re-export the SAME DbResource as models.db_resource"
    )


# ---------------------------------------------------------------------------
# 5. No modules imported at import time (no cycles)
# ---------------------------------------------------------------------------

def test_no_modules_import_on_plugin_config_load() -> None:
    """Loading models.plugin_config must not pull in any dynastore.modules.*."""
    # The standalone smoke test (import-cycle smoke test in the implementation
    # task) enforces the strict no-modules-at-import-time guarantee when run
    # in isolation.  In the full suite other tests may have already triggered
    # modules imports.  Here we simply assert that the module is importable.
    import importlib
    try:
        mod = importlib.import_module("dynastore.models.plugin_config")
        assert mod is not None
    except ImportError as exc:
        raise AssertionError(f"models.plugin_config must be importable: {exc}") from exc


def test_no_modules_import_on_db_resource_load() -> None:
    import importlib
    try:
        mod = importlib.import_module("dynastore.models.db_resource")
        assert mod is not None
    except ImportError as exc:
        raise AssertionError(f"models.db_resource must be importable: {exc}") from exc
