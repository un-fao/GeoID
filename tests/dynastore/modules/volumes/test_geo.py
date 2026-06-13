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

"""Unit tests for the geodetic helpers used by the 3D tiler."""

from __future__ import annotations

import math

from dynastore.modules.volumes.geo import (
    enu_to_ecef_matrix,
    geodetic_to_ecef,
    lonlat_to_enu,
)

# Den Haag, roughly the CityJSON demo centre.
DH_LON, DH_LAT = 4.3007, 52.0705


def _apply_colmajor(m, v):
    """Apply a column-major 4x4 matrix to a homogeneous vec4 -> vec3."""
    e, n, u, w = v
    x = m[0] * e + m[4] * n + m[8] * u + m[12] * w
    y = m[1] * e + m[5] * n + m[9] * u + m[13] * w
    z = m[2] * e + m[6] * n + m[10] * u + m[14] * w
    return (x, y, z)


def test_geodetic_to_ecef_equator_prime_meridian():
    # lon=0, lat=0, h=0 lands on +X axis at the semi-major radius.
    x, y, z = geodetic_to_ecef(0.0, 0.0, 0.0)
    assert math.isclose(x, 6378137.0, rel_tol=1e-9)
    assert abs(y) < 1e-6
    assert abs(z) < 1e-6


def test_geodetic_to_ecef_north_pole():
    # Polar radius b = a*(1-f) ~= 6356752.3 m on the +Z axis.
    x, y, z = geodetic_to_ecef(0.0, 90.0, 0.0)
    assert abs(x) < 1e-3
    assert abs(y) < 1e-3
    assert math.isclose(z, 6356752.314245, rel_tol=1e-9)


def test_geodetic_to_ecef_radius_is_plausible():
    x, y, z = geodetic_to_ecef(DH_LON, DH_LAT, 0.0)
    r = math.sqrt(x * x + y * y + z * z)
    # Geocentric radius is between polar and equatorial radii.
    assert 6_356_000 < r < 6_379_000


def test_origin_maps_to_local_zero():
    e, n, u = lonlat_to_enu(DH_LON, DH_LAT, 0.0, DH_LON, DH_LAT, 0.0)
    assert abs(e) < 1e-6 and abs(n) < 1e-6 and abs(u) < 1e-6


def test_enu_axes_point_the_right_way():
    # A point slightly east has +east, ~0 north. Slightly north has +north.
    e_east, n_east, _ = lonlat_to_enu(DH_LON + 0.001, DH_LAT, 0.0, DH_LON, DH_LAT, 0.0)
    assert e_east > 0
    assert abs(n_east) < abs(e_east) * 1e-3
    e_north, n_north, _ = lonlat_to_enu(DH_LON, DH_LAT + 0.001, 0.0, DH_LON, DH_LAT, 0.0)
    assert n_north > 0
    assert abs(e_north) < abs(n_north) * 1e-3
    # Pure height becomes pure up.
    _, _, u_up = lonlat_to_enu(DH_LON, DH_LAT, 25.0, DH_LON, DH_LAT, 0.0)
    assert math.isclose(u_up, 25.0, rel_tol=1e-6)


def test_local_distance_matches_metres():
    # ~0.001 deg of longitude at this latitude is ~68 m; check it is metric.
    e, _, _ = lonlat_to_enu(DH_LON + 0.001, DH_LAT, 0.0, DH_LON, DH_LAT, 0.0)
    # Spherical equirectangular estimate; exact ENU uses the prime-vertical
    # radius (~0.3% larger at this latitude), so allow 1% — we only assert the
    # result is metric-scale, not that it matches the approximation.
    expected = 0.001 * (math.pi / 180.0) * 6378137.0 * math.cos(math.radians(DH_LAT))
    assert math.isclose(e, expected, rel_tol=1e-2)


def test_transform_inverts_enu_to_ecef():
    # transform · [enu, 1] must reproduce the true ECEF of the point.
    m = enu_to_ecef_matrix(DH_LON, DH_LAT, 0.0)
    lon, lat, h = DH_LON + 0.002, DH_LAT - 0.0015, 30.0
    e, n, u = lonlat_to_enu(lon, lat, h, DH_LON, DH_LAT, 0.0)
    rebuilt = _apply_colmajor(m, (e, n, u, 1.0))
    truth = geodetic_to_ecef(lon, lat, h)
    for a, b in zip(rebuilt, truth):
        assert math.isclose(a, b, rel_tol=1e-7, abs_tol=1e-3)


def test_transform_translation_column_is_origin_ecef():
    m = enu_to_ecef_matrix(DH_LON, DH_LAT, 12.0)
    ox, oy, oz = geodetic_to_ecef(DH_LON, DH_LAT, 12.0)
    assert math.isclose(m[12], ox, rel_tol=1e-9)
    assert math.isclose(m[13], oy, rel_tol=1e-9)
    assert math.isclose(m[14], oz, rel_tol=1e-9)
    assert m[15] == 1.0
