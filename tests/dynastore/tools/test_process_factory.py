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

import pytest
from pydantic import BaseModel, Field
from dynastore.tools.process_factory import create_process_definition
from dynastore.modules.processes.models import ProcessScope

class MockInput(BaseModel):
    required_field: str = Field(..., title="Required Field", description="A required string")
    optional_field: int = Field(10, title="Optional Field", description="An optional integer")

def test_create_process_definition():
    process = create_process_definition(
        id="test-process",
        title="Test Process",
        description="A test process description.",
        input_model=MockInput,
        version="2.0.0",
        scopes=[ProcessScope.COLLECTION],
    )

    assert process.id == "test-process"
    assert process.title == "Test Process"
    assert process.description == "A test process description."
    assert process.version == "2.0.0"
    assert process.scopes == [ProcessScope.COLLECTION]

    inputs = process.inputs
    assert "required_field" in inputs
    # ProcessInput doesn't expose minOccurs directly in this model version
    assert inputs["required_field"].schema_["type"] == "string"

    assert "optional_field" in inputs
    assert inputs["optional_field"].schema_["type"] == "integer"
    assert inputs["optional_field"].schema_["default"] == 10


def test_create_process_definition_rejects_empty_scopes():
    with pytest.raises(ValueError, match="ProcessScope"):
        create_process_definition(
            id="test-process",
            title="Test Process",
            description="A test process description.",
            input_model=MockInput,
            version="2.0.0",
            scopes=[],
        )


def test_create_process_definition_supports_multiple_scopes():
    process = create_process_definition(
        id="multi-scope-process",
        title="Multi-Scope",
        description="Process executable at multiple scopes.",
        input_model=MockInput,
        version="1.0.0",
        scopes=[ProcessScope.CATALOG, ProcessScope.COLLECTION],
    )
    assert process.scopes == [ProcessScope.CATALOG, ProcessScope.COLLECTION]
