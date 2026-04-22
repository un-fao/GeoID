"""Unit tests for dynastore.modules.elasticsearch._serializer.

Regression guard for the production incident (2026-04-22) where the stock
`opensearchpy.serializer.JSONSerializer` crashed on pydantic `HttpUrl`
while indexing a STAC Collection:

    TypeError: Unable to serialize HttpUrl('https://www.esa.int/')
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import BaseModel, HttpUrl

from dynastore.modules.elasticsearch._serializer import CustomOpenSearchSerializer


@pytest.fixture
def serializer() -> CustomOpenSearchSerializer:
    return CustomOpenSearchSerializer()


def _roundtrip(serializer: CustomOpenSearchSerializer, body: object) -> object:
    """Dump with our serializer and parse back with stdlib json."""
    return json.loads(serializer.dumps(body))


def test_httpurl_in_providers_matches_prod_payload(serializer):
    """Exact shape of the failing production document."""
    body = {
        "id": "test_collection_2",
        "providers": [
            {
                "name": {"en": "ESA"},
                "roles": ["producer"],
                "url": HttpUrl("https://www.esa.int/"),
            }
        ],
    }

    result = _roundtrip(serializer, body)

    # HttpUrl is normalized with trailing slash by pydantic.
    assert result["providers"][0]["url"] == "https://www.esa.int/"


def test_pydantic_basemodel_is_model_dumped(serializer):
    class Provider(BaseModel):
        name: str
        url: HttpUrl

    result = _roundtrip(serializer, {"provider": Provider(name="ESA", url=HttpUrl("https://esa.int/"))})

    assert result["provider"] == {"name": "ESA", "url": "https://esa.int/"}


def test_datetime_date_uuid_decimal(serializer):
    body = {
        "started_at": datetime(2015, 6, 23, 12, 0, tzinfo=timezone.utc),
        "day": date(2026, 4, 22),
        "task_id": uuid.UUID("019db277-260a-73a8-9088-a70b6edea0f0"),
        "price": Decimal("3.14"),
        "count": Decimal("10"),  # integer-valued → emitted as int
    }

    result = _roundtrip(serializer, body)

    assert result["started_at"] == "2015-06-23T12:00:00+00:00"
    assert result["day"] == "2026-04-22"
    assert result["task_id"] == "019db277-260a-73a8-9088-a70b6edea0f0"
    assert result["price"] == 3.14
    assert result["count"] == 10


def test_set_is_sorted_list(serializer):
    result = _roundtrip(serializer, {"keywords": {"zulu", "alpha", "mike"}})
    assert result["keywords"] == ["alpha", "mike", "zulu"]


def test_nested_collection_with_httpurl_and_datetime(serializer):
    """STAC-ish shape combining every type we care about."""
    body = {
        "id": "c1",
        "providers": [{"url": HttpUrl("https://example.org/")}],
        "extent": {
            "temporal": {
                "interval": [[datetime(2020, 1, 1, tzinfo=timezone.utc), None]]
            }
        },
        "links": [{"rel": "license", "href": "https://cc.org/by/4.0/"}],
    }

    result = _roundtrip(serializer, body)

    assert result["providers"][0]["url"] == "https://example.org/"
    assert result["extent"]["temporal"]["interval"][0][0] == "2020-01-01T00:00:00+00:00"
    assert result["extent"]["temporal"]["interval"][0][1] is None


def test_plain_string_passes_through_unchanged(serializer):
    # Matches stock JSONSerializer: strings bypass json.dumps.
    assert serializer.dumps("already-json-string") == "already-json-string"


def test_empty_dict_and_list(serializer):
    assert serializer.dumps({}) == "{}"
    assert serializer.dumps([]) == "[]"


def test_regression_plain_json_document(serializer):
    """A boring document must still round-trip byte-for-byte compatible."""
    body = {"a": 1, "b": "two", "c": [1, 2, 3], "d": None, "e": True}
    assert _roundtrip(serializer, body) == body
