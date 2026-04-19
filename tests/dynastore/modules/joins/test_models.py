import pytest
from pydantic import ValidationError

from dynastore.modules.joins.models import JoinRequest


def _minimal_request_dict():
    return {
        "secondary": {"driver": "registered", "ref": "my-bq-collection"},
        "join": {"primary_column": "uid", "secondary_column": "user_id"},
    }


def test_minimal_request_roundtrips():
    req = JoinRequest(**_minimal_request_dict())
    assert req.secondary.driver == "registered"
    assert req.secondary.ref == "my-bq-collection"
    assert req.join.enrichment is True
    assert req.projection.with_geometry is True
    assert req.output.format == "geojson"


def test_unknown_secondary_driver_rejected():
    bad = _minimal_request_dict()
    bad["secondary"]["driver"] = "bogus"
    with pytest.raises(ValidationError):
        JoinRequest(**bad)


def test_extra_field_rejected_at_top_level():
    bad = _minimal_request_dict()
    bad["bogus"] = 1
    with pytest.raises(ValidationError):
        JoinRequest(**bad)


def test_paging_optional_and_clamped():
    req = JoinRequest(**_minimal_request_dict(), paging={"limit": 500, "offset": 0})
    assert req.paging.limit == 500
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), paging={"limit": 0, "offset": 0})
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), paging={"limit": 100000, "offset": 0})


def test_output_format_enum_validated():
    base = _minimal_request_dict()
    JoinRequest(**base, output={"format": "csv"})
    with pytest.raises(ValidationError):
        JoinRequest(**base, output={"format": "xml"})
