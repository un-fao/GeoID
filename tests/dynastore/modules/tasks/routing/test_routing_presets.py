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

"""CloudTaskRoutingPreset / OnpremTaskRoutingPreset register at import time.

Importing ``routing.presets`` runs ``_register()`` (bottom of the module),
publishing both presets into the storage preset registry at
``PresetTier.PLATFORM`` so they surface in the admin presets UI with
dry-run / apply / rollback.
"""
from __future__ import annotations

# Import has the registration side-effect (presets._register() at import).
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import get_preset, list_presets
from dynastore.modules.tasks.routing.presets import (
    CloudTaskRoutingPreset,
    OnpremTaskRoutingPreset,
)


def test_both_presets_registered_by_name():
    names = list_presets()
    assert "cloud" in names
    assert "onprem" in names


def test_get_preset_returns_the_routing_preset_objects():
    assert get_preset("cloud") is CloudTaskRoutingPreset
    assert get_preset("onprem") is OnpremTaskRoutingPreset


def test_presets_declare_platform_tier():
    # tier must be concrete (not None) at registration time, or the platform-tier
    # filter in list_presets / the admin UI would hide them.
    assert CloudTaskRoutingPreset.tier == PresetTier.PLATFORM
    assert OnpremTaskRoutingPreset.tier == PresetTier.PLATFORM


def test_presets_visible_under_platform_tier_filter():
    platform = list_presets(PresetTier.PLATFORM)
    assert "cloud" in platform
    assert "onprem" in platform
