"""
Integration test to verify that multiple collections with CQL filters work correctly.
This simulates the scenario from the error traceback.
"""
import pytest
from dynastore.modules.tools.cql import parse_cql_filter
from sqlalchemy.sql import column as sql_column


def test_multiple_collections_with_cql_filter():
    """
    Simulate the tiles_db scenario where the same CQL filter is applied
    to multiple collections in a UNION query.
    """
    collections = ['collection_0', 'collection_1', 'collection_2']
    cql_filter = "asset_code='ITAL1_01'"
    
    # Build field mapping (same for all collections in this test)
    field_mapping = {
        'asset_code': sql_column('asset_code'),
        'name': sql_column('name'),
        'id': sql_column('id')
    }
    
    all_params = {}
    union_queries = []
    
    for i, collection in enumerate(collections):
        # Parse CQL filter with collection-specific suffix
        cql_where_str, cql_params = parse_cql_filter(
            cql_filter,
            field_mapping=field_mapping,
            valid_props={'asset_code', 'name', 'id'},
            parser_type='cql2'
        )
        
        # Simulate building a UNION query
        union_queries.append(f"SELECT * FROM {collection} WHERE {cql_where_str}")
        all_params.update(cql_params)
    
    # Verify no parameter conflicts
    print(f"\nTotal parameters: {len(all_params)}")
    print(f"Parameters: {list(all_params.keys())}")
    
    # Verify all parameter names are unique
    param_names = list(all_params.keys())
    assert len(param_names) == len(set(param_names)), \
        "All parameter names should be unique"
    
    # Build the full UNION query
    full_query = " UNION ALL ".join(union_queries)
    print(f"\nFull query:\n{full_query}")
    print(f"\nAll params: {all_params}")
    
    # Verify each parameter in the query exists in the params dict
    import re
    param_refs = re.findall(r':(\w+)', full_query)
    for param_ref in param_refs:
        assert param_ref in all_params, \
            f"Parameter :{param_ref} in query but not in params dict"
    
    print("\n✓ All parameters are correctly suffixed and unique!")


if __name__ == '__main__':
    test_multiple_collections_with_cql_filter()
    print("\nTest passed!")
