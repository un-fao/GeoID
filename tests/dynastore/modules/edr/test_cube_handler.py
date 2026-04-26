import pytest
from dynastore.modules.edr.query_handlers.cube import parse_cube_bbox


def test_parse_4_values():
    assert parse_cube_bbox("-10.0,-20.0,30.0,40.0") == (-10.0, -20.0, 30.0, 40.0)


def test_parse_with_spaces():
    assert parse_cube_bbox("0, 0, 1, 1") == (0.0, 0.0, 1.0, 1.0)


def test_parse_6_values_uses_first_4():
    result = parse_cube_bbox("0,0,1,1,100,200")
    assert result == (0.0, 0.0, 1.0, 1.0)


def test_parse_rejects_fewer_than_4():
    with pytest.raises(ValueError, match="bbox must have at least 4"):
        parse_cube_bbox("0,0,1")
