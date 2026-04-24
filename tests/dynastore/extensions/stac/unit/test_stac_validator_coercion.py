"""Unit tests for ``_coerce_for_stac_validation`` and the validator entry points.

Reproduces two real-world warnings observed on collection-create:

1. ``stac-pydantic validation: links.0.title Input should be a valid
   string [type=string_type, input_value={'en': 'CC-BY-4.0 License'}, …]``
   plus the same for ``providers.0.name``.
2. ``pystac validation: object of type 'datetime.datetime' has no len()``
   when ``extent.temporal.interval[0]`` is still a Python ``datetime``
   (as produced by ``model_dump(exclude_unset=True)`` on a Pydantic
   request model that parses ISO inputs to ``datetime``).

The validators are intentionally lenient (warnings, not errors), so the
tests assert on the returned warning list — empty means the coerce step
neutralised the noise; non-empty would mean a real STAC defect leaked
through.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dynastore.extensions.stac.stac_validator import (
    _coerce_for_stac_validation,
    validate_stac_collection,
    validate_stac_item,
)


def test_coerce_flattens_i18n_dict_to_default_lang():
    out = _coerce_for_stac_validation({"en": "ESA", "fr": "ASE"})
    assert out == "ESA"


def test_coerce_falls_back_to_en_when_lang_missing():
    out = _coerce_for_stac_validation({"en": "ESA", "fr": "ASE"}, lang="de")
    assert out == "ESA"


def test_coerce_falls_back_to_first_value_when_en_missing():
    out = _coerce_for_stac_validation({"fr": "ASE", "es": "AEE"}, lang="de")
    assert out in {"ASE", "AEE"}  # dict iteration order on CPython 3.7+


def test_coerce_leaves_non_lang_dicts_intact():
    payload = {"name": "ESA", "url": "https://esa.int"}
    out = _coerce_for_stac_validation(payload)
    assert out == payload


def test_coerce_walks_nested_lists_and_dicts():
    payload = {
        "links": [
            {"rel": "license", "href": "x", "title": {"en": "CC-BY-4.0 License"}}
        ],
        "providers": [{"name": {"en": "ESA"}, "url": "https://esa.int"}],
    }
    out = _coerce_for_stac_validation(payload)
    assert out["links"][0]["title"] == "CC-BY-4.0 License"
    assert out["providers"][0]["name"] == "ESA"
    assert out["providers"][0]["url"] == "https://esa.int"


def test_coerce_isoformats_datetime():
    dt = datetime(2026, 4, 24, 6, 50, tzinfo=timezone.utc)
    out = _coerce_for_stac_validation({"temporal": {"interval": [[dt, None]]}})
    assert out["temporal"]["interval"][0][0] == dt.isoformat()
    assert out["temporal"]["interval"][0][1] is None


def test_coerce_empty_dict_is_not_treated_as_i18n():
    assert _coerce_for_stac_validation({}) == {}


def _minimal_collection() -> dict:
    """Builds a STAC Collection dict that exercises both reported failure
    modes: i18n dicts on ``links[].title`` + ``providers[].name`` and a
    raw ``datetime`` inside ``extent.temporal.interval``."""
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "demo",
        "description": "demo collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {
                "interval": [[datetime(2026, 1, 1, tzinfo=timezone.utc), None]]
            },
        },
        "links": [
            {
                "rel": "license",
                "href": "https://creativecommons.org/licenses/by/4.0/",
                "title": {"en": "CC-BY-4.0 License"},
            }
        ],
        "providers": [
            {"name": {"en": "ESA"}, "roles": ["producer"], "url": "https://esa.int"}
        ],
    }


def test_validate_collection_no_longer_warns_on_i18n_or_datetime():
    """Regression: with the coerce step the two reported warnings disappear."""
    warnings = validate_stac_collection(_minimal_collection())
    blocked = [
        w
        for w in warnings
        if "Input should be a valid string" in w
        or "no len()" in w
    ]
    assert blocked == [], f"unexpected validator noise: {warnings}"


def test_validate_item_handles_i18n_in_links_and_providers():
    item = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "demo-item",
        "collection": "demo",
        "geometry": None,
        "properties": {"datetime": "2026-04-24T06:50:30Z"},
        "links": [
            {"rel": "self", "href": "https://x", "title": {"en": "Self link"}}
        ],
        "assets": {},
    }
    warnings = validate_stac_item(item)
    blocked = [w for w in warnings if "Input should be a valid string" in w]
    assert blocked == [], f"unexpected validator noise: {warnings}"
