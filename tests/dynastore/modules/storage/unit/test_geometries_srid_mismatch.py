"""Regression tests for the trusted pre-processed WKB SRID mismatch guard.

When a feature carries ``wkb_hex_processed`` (the trusted/pre-processed path)
and explicitly declares a source ``srid`` that differs from the collection's
``target_srid``, the sidecar must raise ``SridMismatchError`` immediately.

Silently accepting a mis-projected WKB would corrupt every downstream spatial
query and pre-computed statistic because the pipeline skips re-projection on
this path.

Three cases are covered:
1. Declared source SRID != target_srid  -> SridMismatchError raised.
2. Declared source SRID == target_srid  -> no raise, payload returned.
3. ``srid`` absent entirely (common trusted-path)  -> no raise (no-regression).
"""

import pytest
from shapely import wkb
from shapely.geometry import Polygon

from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.tools.geospatial_exceptions import SridMismatchError


def _make_sidecar(target_srid: int = 4326) -> GeometriesSidecar:
    return GeometriesSidecar(GeometriesSidecarConfig(target_srid=target_srid))


def _square_wkb_hex() -> str:
    poly = Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)])
    return wkb.dumps(poly, hex=True)


class TestTrustedPathSridMismatchGuard:
    def test_declared_srid_mismatch_raises(self):
        """Explicit source SRID that differs from target_srid must be rejected."""
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g1"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            "srid": 4326,  # declared, but collection target is 3857
        }

        with pytest.raises(SridMismatchError) as exc_info:
            sidecar.prepare_upsert_payload(feature, ctx)

        msg = str(exc_info.value)
        assert "4326" in msg
        assert "3857" in msg

    def test_declared_srid_matches_target_no_raise(self):
        """Explicit source SRID equal to target_srid must be accepted."""
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g2"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            "srid": 3857,  # matches target_srid -> OK
        }

        # Must not raise
        result = sidecar.prepare_upsert_payload(feature, ctx)
        assert result is not None

    def test_absent_srid_trusted_path_no_raise(self):
        """Omitting ``srid`` on the trusted path must be accepted unchanged.

        This is the common trusted-path case: the caller asserts the WKB is
        already in the target SRID by not declaring any source SRID.
        """
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g3"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            # no "srid" key — trusted-path contract
        }

        # Must not raise
        result = sidecar.prepare_upsert_payload(feature, ctx)
        assert result is not None
