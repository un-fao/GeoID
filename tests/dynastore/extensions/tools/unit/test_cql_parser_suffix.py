"""
Test to verify that CQL filter parameter suffixing works correctly in tiles_db.
"""
import pytest
from dynastore.modules.tools.cql import parse_cql_filter
from sqlalchemy.sql import column as sql_column


def test_parse_cql_filter_without_suffix():
    """Test that parse_cql_filter works without suffix (backward compatibility)."""
    field_mapping = {
        'asset_code': sql_column('asset_code'),
        'name': sql_column('name')
    }
    
    cql_filter = "asset_code='ITAL1_01'"
    sql_where, params = parse_cql_filter(
        cql_filter,
        field_mapping=field_mapping,
        valid_props={'asset_code', 'name'},
        parser_type='cql2'
    )
    
    # Verify we get valid output
    assert sql_where
    assert params
    
    # Verify no parameter ends with a suffix
    for param_name in params.keys():
        assert not param_name.endswith('_0'), f"Parameter {param_name} should not have suffix"
    
    print(f"SQL: {sql_where}")
    print(f"Params: {params}")
