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

"""Unit tests for ``scripts/validate_config_defaults.py``.

The validator is a CI guardrail against the silent-typo bug class — it must
catch unknown ``class_key`` values, missing keys, non-object values, and
schema mismatches that ``config_seeder`` would otherwise surface only at
boot time on the target environment.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dynastore.scripts import validate_config_defaults as v


def _write(dir_: Path, name: str, payload):
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / name).write_text(
        payload if isinstance(payload, str) else json.dumps(payload)
    )


@pytest.fixture(autouse=True)
def _no_plugin_discovery():
    # The TaskRoutingConfig class must be registered for ``task_routing_config``
    # to resolve. Importing the module is enough — PluginConfig subclasses
    # auto-register via ``PersistentModel.__init_subclass__``. We stub out
    # entry-point discovery to keep the unit tests hermetic, but still ensure
    # the one class the suite exercises is present.
    import dynastore.modules.tasks.routing.model  # noqa: F401  (registration side-effect)

    with patch.object(v, "_discover_plugin_configs", lambda: None):
        yield


def test_clean_dir_returns_zero(tmp_path, capsys):
    _write(tmp_path, "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {}},
    })
    rc = v.main([str(tmp_path)])
    assert rc == 0
    assert "1 file(s) OK" in capsys.readouterr().out


def test_unknown_class_key_returns_one(tmp_path, capsys):
    _write(tmp_path, "bogus.json", {
        "class_key": "NoSuchPluginConfig",
        "value": {"x": 1},
    })
    rc = v.main([str(tmp_path)])
    assert rc == 1
    assert "unknown class_key" in capsys.readouterr().err


def test_pascal_case_hint(tmp_path, capsys):
    _write(tmp_path, "bad.json", {
        "class_key": "TaskRoutingConfig",
        "value": {"tasks": {}},
    })
    rc = v.main([str(tmp_path)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "did you mean 'task_routing_config'" in err


def test_missing_class_key_fails(tmp_path):
    _write(tmp_path, "no-key.json", {"value": {"tasks": {}}})
    assert v.main([str(tmp_path)]) == 1


def test_bad_value_fails(tmp_path):
    _write(tmp_path, "bad.json", {
        "class_key": "task_routing_config",
        "value": "not-an-object",
    })
    assert v.main([str(tmp_path)]) == 1


def test_malformed_json_fails(tmp_path, capsys):
    _write(tmp_path, "bad.json", "{not json")
    rc = v.main([str(tmp_path)])
    assert rc == 1
    assert "unreadable" in capsys.readouterr().err


def test_schema_mismatch_fails(tmp_path, capsys):
    _write(tmp_path, "bad-shape.json", {
        "class_key": "task_routing_config",
        # ``tasks`` must be a dict; passing a list violates the model.
        "value": {"tasks": ["not-a-dict"]},
    })
    rc = v.main([str(tmp_path)])
    assert rc == 1
    assert "does not validate" in capsys.readouterr().err


def test_nonexistent_dir_is_skip_not_fail(tmp_path):
    assert v.main([str(tmp_path / "nope")]) == 0
