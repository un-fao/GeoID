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

"""STAC raster band-type mapping is complete and SSOT-driven (#1216).

``GDAL_TO_STAC_DTYPE`` is keyed on the canonical ``GDAL_BAND_TYPES`` vocabulary
and must cover all of it — the previous capitalized-key map silently dropped
``int8`` / ``int64`` / ``uint64`` to ``None``. The capitalized names gdalinfo
emits resolve through ``canonical_band_type``, so existing blobs keep mapping.
"""
from __future__ import annotations

import pytest

from dynastore.models.field_types import GDAL_BAND_TYPES, canonical_band_type
from dynastore.extensions.stac.metadata_mapper import GDAL_TO_STAC_DTYPE


def test_map_is_complete_over_the_band_vocabulary() -> None:
    # Every canonical band type maps to a STAC DataType — nothing silently None.
    assert set(GDAL_TO_STAC_DTYPE) == GDAL_BAND_TYPES


@pytest.mark.parametrize("band", ["int8", "int64", "uint64"])
def test_previously_dropped_types_now_map(band: str) -> None:
    # Regression: these three had no entry in the old capitalized map.
    assert GDAL_TO_STAC_DTYPE.get(band) is not None
    assert GDAL_TO_STAC_DTYPE[band].value == band


@pytest.mark.parametrize("gdal_name,expected_value", [
    ("Byte", "uint8"),        # GDAL "Byte" -> STAC uint8
    ("Float32", "float32"),
    ("Int16", "int16"),
    ("Int8", "int8"),
    ("UInt64", "uint64"),
])
def test_capitalized_gdal_names_resolve(gdal_name: str, expected_value: str) -> None:
    # The lookup the mapper performs: canonical_band_type() then the SSOT map.
    stac_dt = GDAL_TO_STAC_DTYPE[canonical_band_type(gdal_name)]
    assert stac_dt.value == expected_value
