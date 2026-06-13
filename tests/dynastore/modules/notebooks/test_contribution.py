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

from pathlib import Path

import pytest
from pydantic import ValidationError

from dynastore.models.localization import LocalizedText
from dynastore.modules.notebooks.contribution import NotebookContribution


def test_path_or_content_required():
    with pytest.raises(
        ValidationError, match="Provide one of notebook_path or notebook_content"
    ):
        NotebookContribution(
            notebook_id="x",
            title=LocalizedText(en="X"),
            registered_by="t",
        )


def test_path_and_content_mutually_exclusive(tmp_path: Path):
    nb = tmp_path / "x.ipynb"
    nb.write_text("{}")
    with pytest.raises(ValidationError, match="mutually exclusive"):
        NotebookContribution(
            notebook_id="x",
            title=LocalizedText(en="X"),
            registered_by="t",
            notebook_path=nb,
            notebook_content={},
        )


def test_minimal_path_form(tmp_path: Path):
    nb = tmp_path / "x.ipynb"
    nb.write_text("{}")
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_path=nb,
    )
    assert c.applies_to is None
    assert c.default_catalog_id is None


def test_applies_to_accepts_list_of_ids():
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_content={},
        applies_to=["demo-catalog", "spanner-catalog"],
    )
    assert c.applies_to == ["demo-catalog", "spanner-catalog"]


def test_applies_to_empty_list_normalised_to_none():
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_content={},
        applies_to=[],
    )
    assert c.applies_to is None
