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

"""geometry_column validation at the config/preset boundary.

Covers both ItemsDuckdbDriverConfig and FileBackedPresetParams.  The shared
validation logic (validate_column_identifier from dynastore.tools.db) is the
SSOT; both models delegate to it so invalid identifiers are caught at
parse/apply time rather than at SQL-construction time.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# ItemsDuckdbDriverConfig
# ---------------------------------------------------------------------------

class TestItemsDuckdbDriverConfigGeometryColumn:
    """geometry_column on the driver config model is validated at parse time."""

    def test_valid_default_none_accepted(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        cfg = ItemsDuckdbDriverConfig(format="geoparquet", path="/data/f.parquet")
        assert cfg.geometry_column is None

    def test_valid_standard_name_accepted(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        cfg = ItemsDuckdbDriverConfig(
            format="geoparquet",
            path="/data/f.parquet",
            geometry_column="geometry",
        )
        assert cfg.geometry_column == "geometry"

    def test_valid_non_standard_names_accepted(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        for name in ("geom", "wkb_geometry", "the_geom", "_geom", "Geom", "geom_4326"):
            cfg = ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column=name,
            )
            assert cfg.geometry_column == name

    def test_sql_injection_attempt_rejected(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValidationError) as exc_info:
            ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column='col; DROP TABLE items --',
            )
        assert "geometry_column" in str(exc_info.value).lower() or "sql" in str(exc_info.value).lower()

    def test_quoted_identifier_injection_rejected(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValidationError):
            ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column='a"b',
            )

    def test_leading_digit_rejected(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValidationError):
            ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column="1geom",
            )

    def test_space_in_name_rejected(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValidationError):
            ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column="my geom",
            )

    def test_hyphen_in_name_rejected(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValidationError):
            ItemsDuckdbDriverConfig(
                format="geoparquet",
                path="/data/f.parquet",
                geometry_column="my-geom",
            )


# ---------------------------------------------------------------------------
# FileBackedPresetParams
# ---------------------------------------------------------------------------

class TestFileBackedPresetParamsGeometryColumn:
    """geometry_column on the preset params model is validated at parse time."""

    def test_valid_none_accepted(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        params = FileBackedPresetParams(path="/data/f.parquet", format="geoparquet")
        assert params.geometry_column is None

    def test_valid_standard_name_accepted(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        params = FileBackedPresetParams(
            path="/data/f.parquet",
            format="geoparquet",
            geometry_column="geometry",
        )
        assert params.geometry_column == "geometry"

    def test_valid_non_standard_names_accepted(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        for name in ("geom", "wkb_geometry", "the_geom", "_geom", "Geom", "geom_4326"):
            params = FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column=name,
            )
            assert params.geometry_column == name

    def test_sql_injection_attempt_rejected(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        with pytest.raises(ValidationError) as exc_info:
            FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column='col; DROP TABLE items --',
            )
        assert "geometry_column" in str(exc_info.value).lower() or "sql" in str(exc_info.value).lower()

    def test_quoted_identifier_injection_rejected(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        with pytest.raises(ValidationError):
            FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column='a"b',
            )

    def test_leading_digit_rejected(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        with pytest.raises(ValidationError):
            FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column="1geom",
            )

    def test_space_in_name_rejected(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        with pytest.raises(ValidationError):
            FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column="my geom",
            )

    def test_hyphen_in_name_rejected(self):
        from dynastore.modules.storage.presets.file_backed import FileBackedPresetParams
        with pytest.raises(ValidationError):
            FileBackedPresetParams(
                path="/data/f.parquet",
                format="geoparquet",
                geometry_column="my-geom",
            )
