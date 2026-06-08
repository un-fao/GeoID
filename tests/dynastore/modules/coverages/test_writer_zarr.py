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

import importlib.util
import pytest

_HAS_ZARR = importlib.util.find_spec("zarr") is not None
_HAS_XARRAY = importlib.util.find_spec("xarray") is not None
skipif_no_zarr = pytest.mark.skipif(
    not (_HAS_ZARR and _HAS_XARRAY), reason="zarr and xarray not installed"
)


def test_importable_without_zarr():
    from dynastore.modules.coverages.writers import zarr as zarr_writer
    assert hasattr(zarr_writer, "write_zarr")


@skipif_no_zarr
def test_emits_valid_zip_magic():
    import numpy as np
    from dynastore.modules.coverages.writers.zarr import write_zarr

    arr = np.ones((4, 4), dtype="float32")
    out = b"".join(write_zarr(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["b1"], tiles=iter([(0, 0, arr)]),
    ))
    # ZIP local file header magic
    assert out[:2] == b"PK"


@skipif_no_zarr
def test_multiband_tiles():
    import numpy as np
    from dynastore.modules.coverages.writers.zarr import write_zarr

    arr0 = np.full((4, 4), 1.0, dtype="float32")
    arr1 = np.full((4, 4), 2.0, dtype="float32")
    out = b"".join(write_zarr(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["red", "green"],
        tiles=iter([(0, 0, arr0), (0, 0, arr1)]),
    ))
    assert len(out) > 0
    assert out[:2] == b"PK"


@skipif_no_zarr
def test_3d_tile_multiband():
    import numpy as np
    from dynastore.modules.coverages.writers.zarr import write_zarr

    arr = np.stack([
        np.full((4, 4), 10.0, dtype="float32"),
        np.full((4, 4), 20.0, dtype="float32"),
    ])  # shape (2, 4, 4)
    out = b"".join(write_zarr(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["band1", "band2"],
        tiles=iter([(0, 0, arr)]),
    ))
    assert out[:2] == b"PK"


@skipif_no_zarr
def test_custom_chunk_size():
    import numpy as np
    from dynastore.modules.coverages.writers.zarr import write_zarr

    arr = np.zeros((8, 8), dtype="float32")
    out = b"".join(write_zarr(
        width=8, height=8, bbox=[0.0, 0.0, 8.0, 8.0], crs="EPSG:4326",
        band_names=["b1"], tiles=iter([(0, 0, arr)]),
        chunk_size=4,
    ))
    assert out[:2] == b"PK"
