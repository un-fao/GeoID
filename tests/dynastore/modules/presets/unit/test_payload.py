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

"""Tests for preset payload DTOs and helpers.

Covers:
* ``coerce_payloads`` — all four input shapes plus the error path.
* ``PresetPayload`` / ``PresetChainPayload`` extra="forbid" enforcement.
* ``validate_preset_params`` — happy path and ``ValidationError`` propagation.
* ``load_preset_payloads`` — three file shapes via ``tmp_path`` monkeypatching.
"""
from __future__ import annotations

import json
import pathlib
from unittest.mock import patch

import pytest
from pydantic import BaseModel, ValidationError

from dynastore.modules.presets.payload import (
    PresetChainPayload,
    PresetPayload,
    coerce_payloads,
    validate_preset_params,
)


# ---------------------------------------------------------------------------
# PresetPayload and PresetChainPayload — extra="forbid"
# ---------------------------------------------------------------------------

def test_preset_payload_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        PresetPayload(preset_name="x", typo_field="bad")  # type: ignore[call-arg]


def test_preset_chain_payload_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        PresetChainPayload(
            presets=[{"preset_name": "x"}],
            unexpected="bad",  # type: ignore[call-arg]
        )


def test_preset_chain_payload_rejects_empty_presets() -> None:
    with pytest.raises(ValidationError):
        PresetChainPayload(presets=[])


def test_preset_payload_defaults() -> None:
    p = PresetPayload(preset_name="my_preset")
    assert p.scope_key == "platform"
    assert p.force is False
    assert p.params == {}


# ---------------------------------------------------------------------------
# coerce_payloads — all four input shapes
# ---------------------------------------------------------------------------

def test_coerce_single_payload_instance() -> None:
    p = PresetPayload(preset_name="foo")
    result = coerce_payloads(p)
    assert len(result) == 1
    assert result[0].preset_name == "foo"


def test_coerce_chain_payload_instance() -> None:
    chain = PresetChainPayload(
        presets=[
            PresetPayload(preset_name="a"),
            PresetPayload(preset_name="b"),
        ]
    )
    result = coerce_payloads(chain)
    assert [r.preset_name for r in result] == ["a", "b"]


def test_coerce_single_dict() -> None:
    result = coerce_payloads({"preset_name": "bar", "params": {"x": 1}})
    assert len(result) == 1
    assert result[0].preset_name == "bar"
    assert result[0].params == {"x": 1}


def test_coerce_chain_dict() -> None:
    result = coerce_payloads(
        {"presets": [{"preset_name": "a"}, {"preset_name": "b", "force": True}]}
    )
    assert len(result) == 2
    assert result[0].preset_name == "a"
    assert result[1].force is True


def test_coerce_bare_list_of_dicts() -> None:
    result = coerce_payloads(
        [{"preset_name": "x"}, {"preset_name": "y", "scope_key": "cat:1"}]
    )
    assert len(result) == 2
    assert result[1].scope_key == "cat:1"


def test_coerce_bare_list_of_payload_instances() -> None:
    items = [PresetPayload(preset_name="p1"), PresetPayload(preset_name="p2")]
    result = coerce_payloads(items)
    assert [r.preset_name for r in result] == ["p1", "p2"]


def test_coerce_raises_on_invalid_type() -> None:
    with pytest.raises(ValueError, match="Cannot coerce"):
        coerce_payloads(42)


def test_coerce_raises_on_invalid_dict() -> None:
    """A dict with a typo'd top-level key raises ValueError (extra=forbid)."""
    with pytest.raises(ValueError, match="Invalid preset payload"):
        coerce_payloads({"preset_name": "x", "unknown_key": "oops"})


def test_coerce_raises_on_list_with_bad_element() -> None:
    with pytest.raises(ValueError, match="index 1"):
        coerce_payloads([{"preset_name": "ok"}, "not_a_dict"])


def test_coerce_raises_on_invalid_chain_dict() -> None:
    with pytest.raises(ValueError, match="Invalid preset chain payload"):
        coerce_payloads({"presets": [{"preset_name": "ok"}, {"bad_field": "x"}]})


# ---------------------------------------------------------------------------
# validate_preset_params
# ---------------------------------------------------------------------------

class _StrParams(BaseModel):
    label: str
    count: int = 0


class _MockPreset:
    params_model = _StrParams


def test_validate_preset_params_happy_path() -> None:
    payload = PresetPayload(preset_name="p", params={"label": "hello", "count": 3})
    validated = validate_preset_params(_MockPreset(), payload)
    assert isinstance(validated, _StrParams)
    assert validated.label == "hello"
    assert validated.count == 3


def test_validate_preset_params_propagates_validation_error() -> None:
    payload = PresetPayload(preset_name="p", params={"count": 3})  # missing 'label'
    with pytest.raises(ValidationError):
        validate_preset_params(_MockPreset(), payload)


def test_validate_preset_params_extra_field_ok_when_model_allows() -> None:
    """params dict extra keys are only rejected if the preset's params_model forbids them."""

    class _OpenParams(BaseModel):
        label: str

    class _OpenPreset:
        params_model = _OpenParams

    payload = PresetPayload(preset_name="p", params={"label": "x", "extra": "ok"})
    # _OpenParams has no extra="forbid" so extra keys are ignored by default.
    validated = validate_preset_params(_OpenPreset(), payload)
    assert validated.label == "x"


# ---------------------------------------------------------------------------
# load_preset_payloads — three file shapes via tmp_path
# ---------------------------------------------------------------------------

def test_load_payloads_legacy_single(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "my_preset.json").write_text(
        json.dumps({"preset_name": "my_preset", "params": {"key": "val"}})
    )
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("my_preset")
    assert result is not None
    assert len(result) == 1
    assert result[0]["preset_name"] == "my_preset"
    assert result[0]["params"] == {"key": "val"}


def test_load_payloads_legacy_single_injects_preset_name(tmp_path: pathlib.Path) -> None:
    """When 'preset_name' is absent in the file, it is injected from the file name."""
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "my_preset.json").write_text(
        json.dumps({"params": {"key": "val"}})
    )
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("my_preset")
    assert result is not None
    assert result[0]["preset_name"] == "my_preset"


def test_load_payloads_chain_shape(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "chain.json").write_text(
        json.dumps({"presets": [{"preset_name": "a"}, {"preset_name": "b"}]})
    )
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("chain")
    assert result is not None
    assert len(result) == 2
    assert result[0]["preset_name"] == "a"
    assert result[1]["preset_name"] == "b"


def test_load_payloads_bare_list(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "list.json").write_text(
        json.dumps([{"preset_name": "x"}, {"preset_name": "y", "params": {"n": 1}}])
    )
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("list")
    assert result is not None
    assert len(result) == 2
    assert result[1]["params"] == {"n": 1}


def test_load_payloads_returns_none_when_dir_missing(tmp_path: pathlib.Path) -> None:
    absent_dir = tmp_path / "no_such_dir"
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", absent_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("any_preset")
    assert result is None


def test_load_payloads_returns_none_for_malformed_json(tmp_path: pathlib.Path) -> None:
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "bad.json").write_text("{not valid")
    with patch("dynastore.modules.presets.param_loader.PRESETS_DIR", presets_dir):
        from dynastore.modules.presets.param_loader import load_preset_payloads
        result = load_preset_payloads("bad")
    assert result is None
