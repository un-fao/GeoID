
import pytest
from uuid import uuid4
from shapely.geometry import Point
from shapely import wkb
from dynastore.tools.file_io import _process_records_for_writing
from dynastore.tools.features import Feature

def test_process_records_for_writing_with_dict():
    """Test _process_records_for_writing with a dictionary."""
    geom = Point(0, 0)
    records = [
        {
            "geoid": uuid4(),
            "attributes": {"attr1": "val1"},
            "geom": wkb.dumps(geom),
            "external_id": "ext-1"
        }
    ]
    
    processed = list(_process_records_for_writing(records))
    
    assert len(processed) == 1
    assert processed[0]["type"] == "Feature"
    assert processed[0]["properties"]["attr1"] == "val1"
    assert processed[0]["properties"]["external_id"] == "ext-1"
    assert processed[0]["geometry"]["type"] == "Point"

def test_process_records_for_writing_with_pydantic():
    """Test _process_records_for_writing with a Pydantic model."""
    geoid = uuid4()
    geom = Point(1, 1)
    feature = Feature(
        geoid=geoid,
        attributes={"attr2": "val2"},
        geom=wkb.dumps(geom),
        external_id="ext-2"
    )
    
    records = [feature]
    
    processed = list(_process_records_for_writing(records))
    
    assert len(processed) == 1
    assert processed[0]["type"] == "Feature"
    assert processed[0]["properties"]["attr2"] == "val2"
    assert processed[0]["properties"]["external_id"] == "ext-2"
    assert processed[0]["geometry"]["type"] == "Point"
    assert processed[0]["geometry"]["coordinates"] == (1.0, 1.0)

def test_write_shapefile_with_dict():
    """Test write_shapefile with a dictionary to ensure it doesn't crash."""
    geom = Point(0, 0)
    records = [
        {
            "geoid": uuid4(),
            "attributes": {"attr1": "val1"},
            "geom": wkb.dumps(geom),
            "external_id": "ext-1"
        }
    ]
    
    from dynastore.tools.file_io import write_shapefile
    # Shapefile writer returns a generator of bytes (zip)
    chunks = list(write_shapefile(records, srid=4326))
    
    assert len(chunks) > 0
    full_content = b"".join(chunks)
    assert len(full_content) > 0
    # Basic check it's a ZIP by looking for magic number PK
    assert full_content.startswith(b"PK")
