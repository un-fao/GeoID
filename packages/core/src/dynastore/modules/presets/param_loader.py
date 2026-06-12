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

"""Parametric preset JSON loader.

Reads preset parameters from JSON files under
``${DYNASTORE_CONFIG_ROOT}/presets/<preset_name>.json``, mirroring the
way ``config_seeder`` loads PluginConfig defaults from
``${DYNASTORE_CONFIG_ROOT}/defaults/*.json``.

File shape::

    {
      "preset_name": "my_preset",
      "params": { ... }
    }

``params`` is validated against the preset's ``params_model`` by the
caller.  This module is a pure loader — it performs no DB I/O and has no
IAM or storage imports.

``PRESETS_DIR`` is the resolved path; it mirrors ``DEFAULTS_DIR`` from
``modules/db_config/instance.py``.
"""
from __future__ import annotations

import json
import logging
import pathlib
from typing import Any, Dict, Optional

from dynastore.modules.db_config.instance import CONFIG_ROOT

logger = logging.getLogger(__name__)

PRESETS_DIR: pathlib.Path = CONFIG_ROOT / "presets"


def load_preset_params(preset_name: str) -> Optional[Dict[str, Any]]:
    """Load the ``params`` dict for *preset_name* from the presets directory.

    Returns ``None`` when:
    - ``PRESETS_DIR`` does not exist.
    - No file named ``<preset_name>.json`` is present.
    - The file cannot be read or parsed.

    The caller is responsible for constructing the correct ``params_model``
    instance from the returned dict.
    """
    if not PRESETS_DIR.exists():
        return None

    candidate = PRESETS_DIR / f"{preset_name}.json"
    if not candidate.exists():
        return None

    try:
        payload = json.loads(candidate.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "preset param_loader: could not read %s: %s", candidate, exc
        )
        return None

    if not isinstance(payload, dict):
        logger.warning(
            "preset param_loader: %s must be a JSON object, got %s",
            candidate,
            type(payload).__name__,
        )
        return None

    params = payload.get("params")
    if params is not None and not isinstance(params, dict):
        logger.warning(
            "preset param_loader: %s 'params' must be a JSON object, got %s",
            candidate,
            type(params).__name__,
        )
        return None

    return params  # may be None if key is absent — caller falls back to default
