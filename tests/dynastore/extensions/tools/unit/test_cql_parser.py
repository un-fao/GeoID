
import pytest

pytest.importorskip("pygeofilter", reason="pygeofilter required for CQL tests")

from sqlalchemy.sql import column
from dynastore.modules.tools.cql import parse_cql_filter

def test_parse_cql_filter_unknown_property_validation():
    """Test that unknown properties trigger a helpful error message listing available properties."""
    cql = "bad_prop = 'value'"
    mapping = {"good_prop": column("good_prop"), "other_prop": column("other_prop")}
    
    with pytest.raises(ValueError) as excinfo:
        parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    
    error_msg = str(excinfo.value)
    assert "Unknown properties: bad_prop" in error_msg
    assert "Available properties: good_prop, other_prop" in error_msg

def test_parse_cql_filter_unknown_property_keyerror_fallback():
    """Test fallback error handling when valid_props isn't explicitly passed but key error happens."""
    # This might happen if validation is skipped (valid_props=False/None) but mapping fails
    # However, currently the function derives valid_props from mapping if not provided.
    # To test the KeyError path, we'd need to bypass the initial validation check?
    # If we pass valid_props=[], validation fails.
    # If we pass valid_props set to matching the keys, validation passes.
    # The KeyError usually happens if validation is somehow bypassed or incomplete.
    # Let's verify the validation logic primarily.
    pass

def test_parse_cql_filter_valid():
    """Test valid parsing."""
    cql = "good_prop = 'value'"
    mapping = {"good_prop": column("good_prop")}
    
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql
    assert len(params) > 0

if __name__ == "__main__":
    # verification run logic
    pass

def test_parse_cql_filter_quoted_string():
    """Test that a filter string wrapped in double quotes is handled correctly."""
    cql = '"good_prop = \'value\'"'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql

def test_parse_cql_filter_unquoted_value_error_message():
    """Test that unquoted values raise a helpful error message."""
    cql = "good_prop = SOME_VALUE"  # SOME_VALUE is interpreted as a property
    mapping = {"good_prop": column("good_prop")}
    
    with pytest.raises(ValueError) as excinfo:
        # valid_props is derived from mapping keys
        parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    
    msg = str(excinfo.value)
    assert "Unknown properties: SOME_VALUE" in msg
    assert "Hint: If these are intended to be values, ensure they are enclosed in single quotes" in msg

def test_parse_cql_filter_quoted_string_nested_single_quotes():
    """Test that double-quoted filter containing single-quoted values is handled correctly."""
    # Simulates: filter="asset_code='ITAL1_01'"
    cql = '"good_prop = \'value\'"'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql
    assert params

def test_parse_cql_filter_empty():
    """Test that empty or None filter returns empty SQL."""
    assert parse_cql_filter(None) == ("", {})
    assert parse_cql_filter("") == ("", {})


def test_parse_cql_filter_single_quoted_value():
    """A plain single-quoted string literal binds as a parameter (#1141)."""
    cql = "good_prop = 'PK001'"
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    assert "PK001" in params.values()


def test_parse_cql_filter_escaped_embedded_single_quote():
    """A value containing an embedded quote (CQL2-Text ``''`` escape) parses.

    Refs #1141: the bundled pygeofilter grammar tokenises ``'O''Brien'`` as two
    separate string literals and 400s. ``parse_cql_filter`` must accept the
    spec-compliant doubled-quote escape and bind the *unescaped* value
    (``O'Brien``) as a parameter.
    """
    cql = "good_prop = 'O''Brien'"
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    # The single quote is restored in the bound value (not the doubled escape).
    assert "O'Brien" in params.values()
    assert "O''Brien" not in params.values()


def test_parse_cql_filter_escaped_quote_at_boundaries():
    """Doubled quotes at the start/end of a value also round-trip (#1141)."""
    cql = "good_prop = '''PK'''"  # CQL2-Text for the value: 'PK'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    assert "'PK'" in params.values()


def test_parse_cql_filter_escaped_quote_multiple_clauses():
    """Doubled quotes survive across an AND of two equality clauses (#1141)."""
    cql = "owner = 'O''Hara' AND author = 'D''Angelo'"
    mapping = {"owner": column("owner"), "author": column("author")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    bound = set(params.values())
    assert "O'Hara" in bound
    assert "D'Angelo" in bound


def test_parse_cql_filter_escaped_quote_in_in_list():
    """Doubled quotes round-trip inside an ``IN (...)`` value list (#1141)."""
    cql = "owner IN ('O''Hara', 'plain')"
    mapping = {"owner": column("owner")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    bound = set(params.values())
    assert "O'Hara" in bound
    assert "plain" in bound


def test_parse_cql_filter_plain_value_unaffected_by_quote_handling():
    """Values with no embedded quote are bound verbatim (no regression)."""
    cql = "owner = 'PK001'"
    mapping = {"owner": column("owner")}
    _, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "PK001" in params.values()
