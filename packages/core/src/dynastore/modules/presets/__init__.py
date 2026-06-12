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

"""Neutral platform preset framework.

This package is the platform-level home for preset infrastructure. It has
no IAM or storage-driver imports — those modules import *from* here.

Public surface:

* ``PresetProtocol``         — structural protocol every preset satisfies.
* ``bootstrap_preset_if_absent`` — cold-boot single-application helper.
* ``load_preset_params``     — load JSON params from
  ``${DYNASTORE_CONFIG_ROOT}/presets/<name>.json``.
* ``PRESETS_DIR``            — resolved path (``CONFIG_ROOT / "presets"``).

The registry (``register_preset`` / ``get_preset`` / ``find_preset`` /
``list_presets``) lives in ``modules/storage/presets/registry.py`` and is
re-exported here for convenience.  IAM and storage modules each import the
registry from there; this package provides the unified public alias.
"""
from .protocol import PresetProtocol  # noqa: F401
from .bootstrap import bootstrap_preset_if_absent  # noqa: F401
from .param_loader import load_preset_params, PRESETS_DIR  # noqa: F401

# Re-export the shared registry so callers can import from one place.
from dynastore.modules.storage.presets.registry import (  # noqa: F401
    find_preset,
    get_preset,
    list_presets,
    register_preset,
    search_presets,
)

__all__ = [
    "PresetProtocol",
    "bootstrap_preset_if_absent",
    "load_preset_params",
    "PRESETS_DIR",
    "find_preset",
    "get_preset",
    "list_presets",
    "register_preset",
    "search_presets",
]
