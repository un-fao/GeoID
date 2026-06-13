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
