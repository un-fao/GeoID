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

"""Transitional facade for ``dynastore.extensions``.

The canonical homes for the symbols below are:

- ``dynastore.extensions.protocols`` — ``ExtensionProtocol``
- ``dynastore.extensions.registry``  — ``ExtensionConfig``,
  ``discover_extensions``, ``dynastore_extension``,
  ``get_extension_instance``, ``get_extension_instance_by_class``,
  ``instantiate_extensions``
- ``dynastore.extensions.lifespan``  — ``lifespan``,
  ``apply_app_configurations``

This ``__init__.py`` re-exports them for backward compatibility during
the per-extension wheel migration. It will be removed once all in-tree
callers import from the canonical locations, at which point
``dynastore.extensions`` becomes a PEP 420 namespace package so each
extension wheel can install its own ``dynastore/extensions/<name>/``
subtree.
"""

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.registry import (
    ExtensionConfig,
    discover_extensions,
    dynastore_extension,
    get_extension_instance,
    get_extension_instance_by_class,
    instantiate_extensions,
)
from dynastore.extensions.lifespan import lifespan, apply_app_configurations

__all__ = [
    "ExtensionProtocol",
    "ExtensionConfig",
    "discover_extensions",
    "dynastore_extension",
    "get_extension_instance",
    "get_extension_instance_by_class",
    "instantiate_extensions",
    "lifespan",
    "apply_app_configurations",
]
