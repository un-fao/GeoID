import pytest
from pydantic import BaseModel, Field
from dynastore.tools.process_factory import create_process_definition

class MockInput(BaseModel):
    required_field: str = Field(..., title="Required Field", description="A required string")
    optional_field: int = Field(10, title="Optional Field", description="An optional integer")

def test_create_process_definition():
    process = create_process_definition(
        id="test-process",
        title="Test Process",
        description="A test process description.",
        input_model=MockInput,
        version="2.0.0"
    )

    assert process.id == "test-process"
    assert process.title == "Test Process"
    assert process.description == "A test process description."
    assert process.version == "2.0.0"
    
    inputs = process.inputs
    assert "required_field" in inputs
    # ProcessInput doesn't expose minOccurs directly in this model version
    assert inputs["required_field"].schema_["type"] == "string"
    
    assert "optional_field" in inputs
    assert inputs["optional_field"].schema_["type"] == "integer"
    assert inputs["optional_field"].schema_["default"] == 10
