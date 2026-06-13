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

"""Geodetic helpers for georeferencing the 3D tiler — pure math, stdlib only.

CityJSON footprints are stored as EPSG:4326 lon/lat degrees, but Cesium 3D
Tiles needs Cartesian ECEF metres. We avoid baking absolute ECEF (huge
magnitudes, float32 precision loss) into every vertex by working in a *local
East-North-Up (ENU) metric frame* anchored at a dataset origin, and placing
that frame on the globe with a single 4x4 ``transform`` on the tileset root.

The conversion is exact (no equirectangular approximation):

    enu = R · (ecef(p) − ecef(origin))

where ``R`` is the ENU basis at the origin. The root transform is the exact
inverse mapping (``transform · [e,n,u,1] = ecef(p)``), so a consumer that
honours the transform reconstructs the true position to full double precision.

All angles are degrees on the public API. WGS84 ellipsoid.
"""

from __future__ import annotations

import math
from typing import List, Tuple

# WGS84 ellipsoid parameters.
_WGS84_A = 6378137.0                      # semi-major axis (metres)
_WGS84_F = 1.0 / 298.257223563           # flattening
_WGS84_E2 = _WGS84_F * (2.0 - _WGS84_F)  # first eccentricity squared

Vec3 = Tuple[float, float, float]


def geodetic_to_ecef(lon_deg: float, lat_deg: float, height_m: float = 0.0) -> Vec3:
    """Convert WGS84 geodetic (lon, lat in degrees, height in metres) to ECEF.

    ECEF = Earth-Centred Earth-Fixed (EPSG:4978), metres.
    """
    lon = math.radians(lon_deg)
    lat = math.radians(lat_deg)
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    n = _WGS84_A / math.sqrt(1.0 - _WGS84_E2 * sin_lat * sin_lat)
    x = (n + height_m) * cos_lat * math.cos(lon)
    y = (n + height_m) * cos_lat * math.sin(lon)
    z = (n * (1.0 - _WGS84_E2) + height_m) * sin_lat
    return (x, y, z)


def ecef_to_geodetic(x: float, y: float, z: float) -> Vec3:
    """Inverse of :func:`geodetic_to_ecef` — ECEF metres → (lon, lat, height).

    Uses Bowring's closed-form method (accurate to well under a millimetre for
    terrestrial points). Returns (lon_deg, lat_deg, height_m). Used to recover
    the dataset origin from a tileset root transform's translation column.
    """
    b = _WGS84_A * (1.0 - _WGS84_F)         # semi-minor axis
    ep2 = (_WGS84_A * _WGS84_A - b * b) / (b * b)  # second eccentricity squared
    p = math.sqrt(x * x + y * y)
    lon = math.atan2(y, x)
    if p == 0.0:
        # On the polar axis.
        lat = math.copysign(math.pi / 2.0, z)
        height = abs(z) - b
        return (math.degrees(lon), math.degrees(lat), height)
    theta = math.atan2(z * _WGS84_A, p * b)
    sin_t, cos_t = math.sin(theta), math.cos(theta)
    lat = math.atan2(
        z + ep2 * b * sin_t * sin_t * sin_t,
        p - _WGS84_E2 * _WGS84_A * cos_t * cos_t * cos_t,
    )
    sin_lat = math.sin(lat)
    n = _WGS84_A / math.sqrt(1.0 - _WGS84_E2 * sin_lat * sin_lat)
    height = p / math.cos(lat) - n
    return (math.degrees(lon), math.degrees(lat), height)


def _enu_basis(lon_deg: float, lat_deg: float) -> Tuple[Vec3, Vec3, Vec3]:
    """Return the (east, north, up) unit vectors of the ENU frame at lon/lat.

    Each vector is expressed in ECEF coordinates. They are orthonormal.
    """
    lon = math.radians(lon_deg)
    lat = math.radians(lat_deg)
    sin_lon, cos_lon = math.sin(lon), math.cos(lon)
    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    east = (-sin_lon, cos_lon, 0.0)
    north = (-sin_lat * cos_lon, -sin_lat * sin_lon, cos_lat)
    up = (cos_lat * cos_lon, cos_lat * sin_lon, sin_lat)
    return east, north, up


def lonlat_to_enu(
    lon_deg: float,
    lat_deg: float,
    height_m: float,
    origin_lon_deg: float,
    origin_lat_deg: float,
    origin_height_m: float = 0.0,
) -> Vec3:
    """Exact ENU offset (metres) of a point relative to a local origin.

    Uses ``R · (ecef(p) − ecef(origin))`` with the ENU basis at the origin, so
    there is no small-area approximation error.
    """
    px, py, pz = geodetic_to_ecef(lon_deg, lat_deg, height_m)
    ox, oy, oz = geodetic_to_ecef(origin_lon_deg, origin_lat_deg, origin_height_m)
    dx, dy, dz = px - ox, py - oy, pz - oz
    east, north, up = _enu_basis(origin_lon_deg, origin_lat_deg)
    e = east[0] * dx + east[1] * dy + east[2] * dz
    n = north[0] * dx + north[1] * dy + north[2] * dz
    u = up[0] * dx + up[1] * dy + up[2] * dz
    return (e, n, u)


def enu_to_ecef_matrix(
    origin_lon_deg: float,
    origin_lat_deg: float,
    origin_height_m: float = 0.0,
) -> List[float]:
    """4x4 matrix mapping local ENU metres → ECEF, **column-major** (16 floats).

    Column-major is the convention used by glTF and the Cesium 3D Tiles
    ``transform`` member. The columns are ``[east, north, up, ecef(origin)]``::

        transform · [e, n, u, 1]ᵀ = ecef(origin) + e·east + n·north + u·up
                                   = ecef(point)
    """
    east, north, up = _enu_basis(origin_lon_deg, origin_lat_deg)
    ox, oy, oz = geodetic_to_ecef(origin_lon_deg, origin_lat_deg, origin_height_m)
    # Column-major: col0, col1, col2, col3 laid out contiguously.
    return [
        east[0], east[1], east[2], 0.0,
        north[0], north[1], north[2], 0.0,
        up[0], up[1], up[2], 0.0,
        ox, oy, oz, 1.0,
    ]
