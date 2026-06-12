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

"""Notebook contribution contract tests for the Moving Features extension."""
from __future__ import annotations

import importlib

import pytest


def test_moving_features_notebooks_build_contributions() -> None:
    try:
        mod = importlib.import_module("dynastore.extensions.moving_features.notebooks")
    except ModuleNotFoundError as exc:
        pytest.skip(f"moving_features extension not installed: {exc}")

    contribs = mod.build_contributions()
    assert isinstance(contribs, list)
    assert len(contribs) >= 1, "Expected at least one notebook contribution from moving_features"
    for c in contribs:
        assert c.notebook_id, "notebook_id must be non-empty"
        assert c.notebook_path, "notebook_path must be set"
        assert c.registered_by, "registered_by is required"
        assert c.notebook_path.exists(), f"Notebook file not found: {c.notebook_path}"
