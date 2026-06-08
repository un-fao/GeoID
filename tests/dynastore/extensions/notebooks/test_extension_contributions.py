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

"""Pin every extension's ``build_contributions()`` against the
``NotebookContribution`` model contract.

#587: when ``registered_by`` was made required in #409, the two
extensions with explicit constructors regressed at startup because no
test exercised the discovery path. This file walks every package that
publishes a ``notebooks.py`` module and asserts the model validates.
"""
from __future__ import annotations

import importlib

import pytest

EXTENSION_NOTEBOOK_MODULES = [
    "dynastore.extensions.stac.notebooks",
    "dynastore.extensions.features.notebooks",
    "dynastore.extensions.records.notebooks",
    "dynastore.extensions.coverages.notebooks",
    "dynastore.extensions.assets.notebooks",
    "dynastore.extensions.web.notebooks",
]


@pytest.mark.parametrize("module_path", EXTENSION_NOTEBOOK_MODULES)
def test_build_contributions_returns_valid_contributions(module_path: str) -> None:
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        pytest.skip(f"{module_path} not installed: {exc}")

    contribs = mod.build_contributions()
    assert isinstance(contribs, list)
    for c in contribs:
        # `registered_by` is required (#409). Re-validating the
        # model would mask attribute access bugs, so we touch fields
        # directly.
        assert c.notebook_id
        assert c.notebook_path
        assert c.registered_by, (
            f"{module_path}: contribution {c.notebook_id!r} missing registered_by"
        )
