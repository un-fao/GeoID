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

"""Regression tests for the trusted pre-processed WKB SRID mismatch guard.

When a feature carries ``wkb_hex_processed`` (the trusted/pre-processed path)
and explicitly declares a source ``srid`` that differs from the collection's
``target_srid``, the sidecar must raise ``SridMismatchError`` immediately.

Silently accepting a mis-projected WKB would corrupt every downstream spatial
query and pre-computed statistic because the pipeline skips re-projection on
this path.

Cases covered:
1. Declared source SRID (int) != target_srid  -> SridMismatchError raised.
2. Declared source SRID (int) == target_srid  -> no raise, payload returned.
3. ``srid`` absent entirely (common trusted-path)  -> no raise (no-regression).
4. Declared source SRID as STRING matching target_srid -> no raise (coercion).
5. Declared source SRID as STRING not matching target_srid -> SridMismatchError.
6. Declared source SRID as unparseable string -> SridMismatchError.
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

    def test_declared_srid_string_matching_target_no_raise(self):
        """String SRID equal to target_srid after int coercion must be accepted."""
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g4"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            "srid": "3857",  # string, but coerces to int 3857 == target_srid
        }

        # Must not raise
        result = sidecar.prepare_upsert_payload(feature, ctx)
        assert result is not None

    def test_declared_srid_string_mismatch_raises(self):
        """String SRID that, after coercion, differs from target_srid must be rejected."""
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g5"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            "srid": "4326",  # coerces to int 4326 != 3857
        }

        with pytest.raises(SridMismatchError) as exc_info:
            sidecar.prepare_upsert_payload(feature, ctx)

        msg = str(exc_info.value)
        assert "4326" in msg
        assert "3857" in msg

    def test_declared_srid_unparseable_string_raises(self):
        """A garbage SRID value that cannot be coerced to int must raise SridMismatchError."""
        sidecar = _make_sidecar(target_srid=3857)
        ctx: dict = {"geoid": "g6"}
        feature = {
            "wkb_hex_processed": _square_wkb_hex(),
            "geom_type": "Polygon",
            "srid": "abc",  # not a valid integer
        }

        with pytest.raises(SridMismatchError) as exc_info:
            sidecar.prepare_upsert_payload(feature, ctx)

        msg = str(exc_info.value)
        assert "unparseable" in msg
