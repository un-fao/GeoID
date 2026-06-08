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

import pytest
from dynastore.modules.coverages.writers import writer_for, MEDIA_TYPE_FOR


def test_media_type_map():
    assert MEDIA_TYPE_FOR["geotiff"] == "image/tiff;application=geotiff"
    assert MEDIA_TYPE_FOR["netcdf"] == "application/x-netcdf"
    assert MEDIA_TYPE_FOR["zarr"] == "application/x-zarr"
    assert MEDIA_TYPE_FOR["covjson"] == "application/prs.coverage+json"


def test_writer_for_covjson_is_pure_callable():
    w = writer_for("covjson")
    assert callable(w)


def test_writer_for_netcdf_is_callable():
    w = writer_for("netcdf")
    assert callable(w)


def test_writer_for_zarr_is_callable():
    w = writer_for("zarr")
    assert callable(w)


def test_writer_for_unknown_raises():
    with pytest.raises(ValueError):
        writer_for("bogus")
