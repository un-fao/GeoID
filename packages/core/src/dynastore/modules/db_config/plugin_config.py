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

"""Backward-compatibility re-export shim — #1555.

The canonical location of ``PluginConfig`` and its registry helpers is now
``dynastore.models.plugin_config``.  This shim keeps the previous import
path ``dynastore.modules.db_config.plugin_config`` working unchanged so
that existing callers require no edits.

Do not add new logic here; extend ``dynastore.models.plugin_config`` instead.
"""

from dynastore.models.plugin_config import (  # noqa: F401
    PluginConfig,
    resolve_config_class,
    require_config_class,
    list_registered_configs,
    _collect_required_fields,
    _APPLY_HANDLERS,
    _VALIDATE_HANDLERS,
)

__all__ = [
    "PluginConfig",
    "resolve_config_class",
    "require_config_class",
    "list_registered_configs",
    "_collect_required_fields",
    "_APPLY_HANDLERS",
    "_VALIDATE_HANDLERS",
]
