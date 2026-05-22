"""Unit tests for the STAC /search ES fast-path read contract.

The ES dispatch returns raw indexed ``_source`` dicts (attributes nested under
``properties.extras``, internal ``_*`` fields and leaked echo keys at the top
level, possibly empty geometry). ``_es_hits_to_features`` must restore the
GeoJSON/STAC read contract — matching the GET items / PostgreSQL output — and
inject the ``_catalog_id`` / ``_collection_id`` properties the serializer reads.

Regression guard for #1247 surfacing on the STAC /search endpoint.
"""
from __future__ import annotations

from dynastore.extensions.stac.search import _es_hits_to_features


def _malformed_hit() -> dict:
    return {
        "type": "Feature",
        "id": "325",
        "geometry": {},
        "collection": "glosis_demo",
        "catalog_id": "datamgr01",
        "_external_id": "325",
        "_asset_id": "ALBL1_01",
        "properties": {
            "extras": {"CODE": "325", "NAME": "Lushnje", "area": 580580547.78}
        },
        # leaked top-level echo
        "CODE": "325",
        "NAME": "Lushnje",
        "area": 580580547.78,
    }


def test_unprojects_and_injects_catalog_collection() -> None:
    feats = _es_hits_to_features([_malformed_hit()], "datamgr01", ["glosis_demo"])
    assert len(feats) == 1
    out = feats[0].model_dump(exclude_none=True, by_alias=True)

    assert out["id"] == "325"
    assert "extras" not in out["properties"]
    assert out["properties"]["CODE"] == "325"
    assert out["properties"]["NAME"] == "Lushnje"
    assert out["properties"]["area"] == 580580547.78
    # serializer contract injected.
    assert out["properties"]["_catalog_id"] == "datamgr01"
    assert out["properties"]["_collection_id"] == "glosis_demo"
    # no top-level attribute echo / internal / catalog_id leak.
    for leaked in ("CODE", "NAME", "area", "catalog_id", "_external_id", "_asset_id"):
        assert leaked not in out
    # empty geometry normalised to null (dropped by exclude_none).
    assert "geometry" not in out or out["geometry"] is None


def test_collection_id_falls_back_to_source_collection_when_multi_scope() -> None:
    hit = _malformed_hit()
    hit["collection"] = "glosis_demo"
    # multiple collections requested → cannot infer from cids, use _source.
    feats = _es_hits_to_features([hit], "datamgr01", ["a", "b"])
    out = feats[0].model_dump(exclude_none=True, by_alias=True)
    assert out["properties"]["_collection_id"] == "glosis_demo"


def test_already_feature_passthrough() -> None:
    from dynastore.models.shared_models import Feature

    f = Feature.model_validate(
        {"type": "Feature", "id": "1", "properties": {"k": "v"}}
    )
    feats = _es_hits_to_features([f], "cat", ["col"])
    assert feats == [f]


def test_malformed_hit_skipped_not_raised() -> None:
    # missing required id → skipped, not raised.
    feats = _es_hits_to_features(
        [{"type": "Feature", "properties": {"extras": {"a": 1}}}],
        "cat",
        ["col"],
    )
    assert feats == []
