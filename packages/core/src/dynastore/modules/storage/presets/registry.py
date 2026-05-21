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

"""Routing preset registry (#847).

Plain dict-backed registry. Built-in presets register themselves on
package import (see ``__init__``). Extensions register additional
presets during module bootstrap by importing ``register_preset``.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .protocol import PresetTier, RoutingPreset

_REGISTRY: Dict[str, RoutingPreset] = {}


def register_preset(preset: RoutingPreset) -> None:
    """Register a preset under ``preset.name``.

    Raises ``ValueError`` if a preset with that name is already
    registered — extensions opting in to a name owned by core must use
    a different name. Re-registration to override would mask config
    surprises; explicit error is preferred.
    """
    if preset.name in _REGISTRY:
        raise ValueError(
            f"Preset {preset.name!r} already registered "
            f"(by {type(_REGISTRY[preset.name]).__name__})"
        )
    _REGISTRY[preset.name] = preset


def get_preset(name: str) -> RoutingPreset:
    """Look up a preset by name. Raises ``KeyError`` with the available
    presets in the message for operator self-service."""
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown preset {name!r}. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def list_presets(tier: Optional[PresetTier] = None) -> List[str]:
    """Sorted list of registered preset names.

    When ``tier`` is given, only presets declaring that tier are
    returned — the registry stays a single flat namespace, the filter is
    applied at read time.
    """
    if tier is None:
        return sorted(_REGISTRY)
    return sorted(
        name
        for name, preset in _REGISTRY.items()
        if getattr(preset, "tier", None) == tier
    )
