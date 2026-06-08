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

"""Decorator registry mapping `kind` → provider factory."""

from __future__ import annotations

from typing import Any, Callable, Dict

from .base import HierarchyProvider
from .config import HierarchyProviderConfig


ProviderFactory = Callable[[HierarchyProviderConfig, Any], HierarchyProvider]

_REGISTRY: Dict[str, ProviderFactory] = {}


def register_provider(kind: str) -> Callable[[ProviderFactory], ProviderFactory]:
    """Decorator: register a factory for `kind`.

    @register_provider("static")
    def _build_static(config, ctx): ...
    """

    def _decorate(factory: ProviderFactory) -> ProviderFactory:
        if kind in _REGISTRY:
            raise ValueError(f"provider kind already registered: {kind!r}")
        _REGISTRY[kind] = factory
        return factory

    return _decorate


def get_hierarchy_provider(
    config: HierarchyProviderConfig,
    ctx: Any,
) -> HierarchyProvider:
    """Dispatch on `config.kind` to the registered factory."""
    try:
        factory = _REGISTRY[config.kind]
    except KeyError as exc:
        raise LookupError(
            f"no provider registered for kind={config.kind!r}; "
            f"known kinds: {sorted(_REGISTRY)}"
        ) from exc
    return factory(config, ctx)
