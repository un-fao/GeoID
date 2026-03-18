
import pytest
from pydantic import BaseModel, ValidationError, Field, create_model, ConfigDict
from typing import List, Optional, Any, Dict, Type

# --- Mocking dependencies for the test ---

class AttributeSchemaEntry(BaseModel):
    name: str
    type: str
    required: bool = True
    default: Optional[Any] = None

def get_attribute_validator_model(schema: List[AttributeSchemaEntry]) -> Optional[Type[BaseModel]]:
    if not schema:
        return None
    fields: Dict[str, Any] = {}
    type_map = {
        "string": str, "number": float, "integer": int,
        "boolean": bool,
        "array": list, "object": dict,
    }
    for entry in schema:
        field_type = type_map.get(entry.type, Any)
        fields[entry.name] = (Optional[field_type], entry.default) if not entry.required else (field_type, ...)
    
    return create_model('DynamicAttributeValidator', **fields, __config__=ConfigDict(extra='ignore'))

class AttributeMappingItem(BaseModel):
    source: str
    map_to: str

def map_record_attributes(raw_attributes: dict, attribute_mapping: Optional[List[AttributeMappingItem]]) -> dict:
    if not attribute_mapping: return raw_attributes
    mapped_attributes = {}
    for item in attribute_mapping:
        if item.source in raw_attributes: mapped_attributes[item.map_to] = raw_attributes[item.source]
    return mapped_attributes

# --- The Test Case ---

def test_mapped_away_required_attribute_validation_error():
    """
    Test that validates the scenario where a required attribute (ADM0_NAME)
    is present in the source but renamed (mapped to NAME) in the destination,
    causing the validator (which still expects ADM0_NAME) to fail.
    
    The goal is to assert that the improved error handling logic (to be implemented)
    provides a helpful hint.
    """
    
    # 1. Schema: Expects ADM0_NAME
    schema_entries = [
        AttributeSchemaEntry(name="ADM0_NAME", type="string", required=True),
    ]
    Validator = get_attribute_validator_model(schema_entries)
    
    # 2. Source Data: Has ADM0_NAME
    raw_record = {
        'ADM0_NAME': 'Italy',
        'OTHER': 'Value'
    }
    
    # 3. Mapping: Renames ADM0_NAME -> NAME
    mapping_config = [
        AttributeMappingItem(source="ADM0_NAME", map_to="NAME"),
    ]
    
    # 4. Map attributes
    attributes_for_db = map_record_attributes(raw_record, mapping_config)
    # attributes_for_db is now {'NAME': 'Italy'}
    
    # 5. Validate (Should Fail)
    # We want to catch the error and verify our proposed logic can detect the mismatch
    
    # Emulate the logic we want to insert into main_ingestion.py
    try:
        Validator.model_validate(attributes_for_db).model_dump(exclude_unset=True)
    except ValidationError as e:
        error_str = str(e)
        
        # --- This is the logic we plan to add to main_ingestion.py ---
        # We need to simulate the 'hint' generation to prove it works
        
        hints = []
        for error in e.errors():
            if error['type'] == 'missing':
                # The field name in pydantic errors for top-level models is the first part of loc
                missing_field = error['loc'][0]
                if missing_field in raw_record and missing_field not in attributes_for_db:
                     hints.append(f"Attribute '{missing_field}' is required by the schema but was not found in the mapped attributes. It exists in the source data; check your column_mapping.")
        
        if hints:
            full_error_msg = f"{error_str}\nHints: {'; '.join(hints)}"
        else:
            full_error_msg = error_str
            
        print(f"\nGenererated Error:\n{full_error_msg}")
        
        # Assertion: Check if our hint logic actually found the issue
        assert "Attribute 'ADM0_NAME' is required by the schema" in full_error_msg
        assert "check your column_mapping" in full_error_msg
        return

    pytest.fail("Validation should have failed but succeeded.")

if __name__ == "__main__":
    test_mapped_away_required_attribute_validation_error()
