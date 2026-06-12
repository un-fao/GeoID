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

"""Tests for the preset JSON param loader.

All tests use a temporary directory — no real filesystem writes outside /tmp.
"""
from __future__ import annotations

import json
import pathlib
from unittest.mock import patch

import pytest


def test_load_returns_none_when_dir_missing(tmp_path: pathlib.Path) -> None:
    absent_dir = tmp_path / "no_such_dir"
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", absent_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("some_preset")
    assert result is None


def test_load_returns_none_when_file_missing(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("my_preset")
    assert result is None


def test_load_returns_params_dict(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    payload = {"preset_name": "my_preset", "params": {"key": "value", "count": 3}}
    (presets_dir / "my_preset.json").write_text(json.dumps(payload))

    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("my_preset")

    assert result == {"key": "value", "count": 3}


def test_load_returns_none_for_absent_params_key(tmp_path: pathlib.Path) -> None:
    """A valid JSON file with no 'params' key returns None."""
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "my_preset.json").write_text(json.dumps({"preset_name": "my_preset"}))

    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("my_preset")
    assert result is None


def test_load_returns_none_for_malformed_json(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "bad_preset.json").write_text("{not valid json")

    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("bad_preset")
    assert result is None


def test_load_returns_none_when_params_not_dict(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "list_preset.json").write_text(json.dumps({"params": ["a", "b"]}))

    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_params
        result = load_preset_params("list_preset")
    assert result is None


def test_presets_dir_respects_env_var(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PRESETS_DIR is derived from DYNASTORE_CONFIG_ROOT/presets."""
    monkeypatch.setenv("DYNASTORE_CONFIG_ROOT", str(tmp_path))
    # Re-import to re-evaluate the module-level constant.
    import importlib
    import dynastore.modules.db_config.instance as inst_mod
    importlib.reload(inst_mod)
    import dynastore.modules.presets.param_loader as loader_mod
    importlib.reload(loader_mod)

    assert loader_mod.PRESETS_DIR == tmp_path / "presets"
