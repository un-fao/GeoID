"""Regression: GeometriesSidecar.prepare_upsert_payload must propagate
the geometry-derived ``content_hash`` into the per-item context dict.

Without this propagation the orchestrator's ``hub_payload`` never carries
``content_hash``, the hub column stays NULL on every insert, and the
``CONTENT_HASH`` identity matcher plus ``skip_if_unchanged_content_hash``
gate become dead code paths.

Two paths are exercised:

* Standard path (no ``wkb_hex_processed`` on the feature) — the sidecar
  routes through ``process_geometry`` which itself computes the hash;
  the test asserts that hash is forwarded into the context unchanged.
* Trusted path (feature carries a pre-processed WKB hex from the
  ingestion stage) — ``process_geometry`` is skipped, so the sidecar
  must derive the hash locally from the final WKB to keep the invariant
  that the persisted hash matches the persisted geometry.
"""

import hashlib

from shapely import wkb
from shapely.geometry import Polygon

from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)


def _make_sidecar() -> GeometriesSidecar:
    return GeometriesSidecar(GeometriesSidecarConfig())


def _square_wkb_hex() -> str:
    poly = Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)])
    return wkb.dumps(poly, hex=True)


class TestContentHashPropagation:
    def test_standard_path_propagates_process_geometry_hash(self):
        sidecar = _make_sidecar()
        ctx: dict = {"geoid": "g1"}
        feature = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
                ],
            },
        }

        sidecar.prepare_upsert_payload(feature, ctx)

        assert "content_hash" in ctx, "sidecar did not propagate content_hash"
        assert isinstance(ctx["content_hash"], str)
        assert len(ctx["content_hash"]) == 64  # sha256 hex digest

    def test_trusted_path_derives_hash_from_final_wkb(self):
        sidecar = _make_sidecar()
        ctx: dict = {"geoid": "g2"}
        wkb_hex = _square_wkb_hex()
        # Trusted ingestion contract: geometry already processed upstream,
        # final WKB hex carried on the feature so process_geometry is bypassed.
        feature = {
            "wkb_hex_processed": wkb_hex,
            "geom_type": "Polygon",
            "bbox_coords": [0.0, 0.0, 1.0, 1.0],
        }

        sidecar.prepare_upsert_payload(feature, ctx)

        expected = hashlib.sha256(bytes.fromhex(wkb_hex)).hexdigest()
        assert ctx.get("content_hash") == expected

    def test_two_identical_features_yield_same_hash(self):
        sidecar = _make_sidecar()
        feat = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]]
                ],
            },
        }
        ctx_a: dict = {"geoid": "a"}
        ctx_b: dict = {"geoid": "b"}

        sidecar.prepare_upsert_payload(feat, ctx_a)
        sidecar.prepare_upsert_payload(feat, ctx_b)

        assert ctx_a["content_hash"] == ctx_b["content_hash"]

    def test_distinct_geometries_yield_distinct_hashes(self):
        sidecar = _make_sidecar()
        ctx_a: dict = {"geoid": "a"}
        ctx_b: dict = {"geoid": "b"}
        feat_a = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
                ],
            },
        }
        feat_b = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0], [0.0, 0.0]]
                ],
            },
        }

        sidecar.prepare_upsert_payload(feat_a, ctx_a)
        sidecar.prepare_upsert_payload(feat_b, ctx_b)

        assert ctx_a["content_hash"] != ctx_b["content_hash"]
