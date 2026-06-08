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

"""Writer dispatch by output format."""

from __future__ import annotations

from typing import Callable, Dict


MEDIA_TYPE_FOR: Dict[str, str] = {
    "geotiff": "image/tiff;application=geotiff",
    "netcdf":  "application/x-netcdf",
    "zarr":    "application/x-zarr",
    "covjson": "application/prs.coverage+json",
}


def writer_for(fmt: str) -> Callable:
    if fmt == "covjson":
        from .coveragejson import write_coveragejson
        return write_coveragejson
    if fmt == "geotiff":
        from .geotiff import write_geotiff
        return write_geotiff
    if fmt == "netcdf":
        from .netcdf import write_netcdf
        return write_netcdf
    if fmt == "zarr":
        from .zarr import write_zarr
        return write_zarr
    raise ValueError(f"Unknown coverage output format: {fmt!r}")
