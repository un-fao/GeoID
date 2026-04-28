"""``BucketService.generate_bucket_name`` produces project-id-prefixed,
length-safe bucket names.

The previous opaque-hash scheme produced names like ``d88971-test-...``
that were error-prone to type or recognise (e.g. the ``dd88971`` typo
in production).  The new scheme uses the project ID directly so the
bucket name is human-recognisable in logs / consoles / report paths.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dynastore.modules.gcp.bucket_service import BucketService


def _make_bm(project_id: str) -> BucketService:
    return BucketService(
        engine=None,
        config_service=None,
        storage_client=MagicMock(),
        project_id=project_id,
        region="europe-west1",
    )


def test_basic_naming_uses_project_id_prefix():
    bm = _make_bm("fao-aip-geospatial-review")
    assert bm.generate_bucket_name("test_catalog_19") == \
        "fao-aip-geospatial-review-test-catalog-19"


def test_physical_schema_preferred_over_catalog_id():
    bm = _make_bm("fao-aip-geospatial-review")
    assert bm.generate_bucket_name("any-id", physical_schema="s_2ka8fbc3") == \
        "fao-aip-geospatial-review-s-2ka8fbc3"


def test_underscores_normalised_to_dashes():
    bm = _make_bm("fao-aip-geospatial-review")
    assert bm.generate_bucket_name("with_under_scores") == \
        "fao-aip-geospatial-review-with-under-scores"


def test_uppercase_lowercased():
    bm = _make_bm("FAO-AIP-Geospatial-Review")
    name = bm.generate_bucket_name("TEST_CATALOG_19")
    assert name == name.lower()
    assert name.startswith("fao-aip-geospatial-review-")


def test_within_63_char_limit_keeps_full_identifier():
    bm = _make_bm("fao-aip-geospatial-review")
    # Identifier picked so total length is exactly 63
    pad = "x" * (63 - len("fao-aip-geospatial-review-"))  # 37 chars
    name = bm.generate_bucket_name(pad)
    assert name == f"fao-aip-geospatial-review-{pad}"
    assert len(name) == 63


def test_overflow_truncates_identifier_and_appends_hash():
    bm = _make_bm("fao-aip-geospatial-review")
    long_id = "x" * 100
    name = bm.generate_bucket_name(long_id)
    assert len(name) <= 63
    assert name.startswith("fao-aip-geospatial-review-")
    # Suffix should be 8 hex chars after a dash
    assert name[-9] == "-"
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_overflow_is_deterministic_for_same_identifier():
    bm = _make_bm("fao-aip-geospatial-review")
    long_id = "y" * 200
    n1 = bm.generate_bucket_name(long_id)
    n2 = bm.generate_bucket_name(long_id)
    assert n1 == n2


def test_overflow_differentiates_distinct_identifiers():
    bm = _make_bm("fao-aip-geospatial-review")
    n1 = bm.generate_bucket_name("z" * 100)
    n2 = bm.generate_bucket_name("z" * 99 + "a")  # different last char
    assert n1 != n2  # the appended hash makes them distinct


def test_short_project_id_works():
    bm = _make_bm("p")
    name = bm.generate_bucket_name("test_catalog_19")
    assert name == "p-test-catalog-19"


def test_missing_project_id_raises():
    bm = _make_bm("")
    with pytest.raises(RuntimeError, match="GCP Project ID not available"):
        bm.generate_bucket_name("test_catalog_19")


def test_long_identifier_keeps_full_project_name():
    bm = _make_bm("fao-aip-geospatial-review")  # 25 chars
    name = bm.generate_bucket_name("x" * 100)
    assert name.startswith("fao-aip-geospatial-review-")  # prefix preserved
    assert len(name) <= 63
    assert name[-9] == "-"  # 8-char hash after dash
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_long_project_id_truncated_with_hash():
    # Project ID > 2/3 of 63 (42) triggers Path B truncation.
    long_proj = "a" * 50  # exceeds project_min*2 (42)
    bm = _make_bm(long_proj)
    name = bm.generate_bucket_name("test_catalog_19")
    assert len(name) <= 63
    # Project segment should be ~1/3 budget = 21 chars: 12 readable + '-' + 8 hex
    assert name.startswith("a" * 12 + "-")
    proj_hash = name[:21].rsplit("-", 1)[1]
    assert len(proj_hash) == 8
    assert all(c in "0123456789abcdef" for c in proj_hash)


def test_long_project_and_long_identifier_split_one_third_two_thirds():
    long_proj = "p" * 50
    bm = _make_bm(long_proj)
    name = bm.generate_bucket_name("i" * 100)
    assert len(name) <= 63
    # Truncated project section ~21 chars (1/3 budget), identifier section ~42 chars (2/3)
    project_section = name[:21]
    assert project_section.startswith("p" * 12 + "-")  # 12 head + '-' + 8 hash
    # Trailing 8 chars of full bucket are the identifier hash
    assert name[-9] == "-"
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_distinct_long_identifiers_never_collide():
    bm = _make_bm("fao-aip-geospatial-review")
    n1 = bm.generate_bucket_name("a" * 200)
    n2 = bm.generate_bucket_name("a" * 199 + "b")
    assert n1 != n2
