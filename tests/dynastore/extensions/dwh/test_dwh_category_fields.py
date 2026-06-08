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

"""DWH join request model — three-category field selection (properties/stats/system).

Pins the clean replacement of the old ``geospatial_attributes`` + ``attributes``
fields with the storage-agnostic ``properties`` / ``stats`` / ``system`` lists.
"""

from dynastore.extensions.dwh.models import DWHJoinRequestBase
from dynastore.models.shared_models import OutputFormatEnum


def _base_kwargs(**overrides):
    kwargs = dict(
        dwh_project_id="proj",
        dwh_query="SELECT 1",
        collection="col1",
        dwh_join_column="external_id",
        join_column="external_id",
    )
    kwargs.update(overrides)
    return kwargs


class TestDWHJoinRequestBaseModel:
    def test_new_fields_accepted(self):
        req = DWHJoinRequestBase(
            **_base_kwargs(
                properties=["code", "region"],
                stats=["area_ha"],
                system=["external_id"],
            )
        )
        assert req.properties == ["code", "region"]
        assert req.stats == ["area_ha"]
        assert req.system == ["external_id"]

    def test_wildcard_fields_accepted(self):
        req = DWHJoinRequestBase(
            **_base_kwargs(properties=["*"], stats=["*"], system=["*"])
        )
        assert req.properties == ["*"]
        assert req.stats == ["*"]
        assert req.system == ["*"]

    def test_optional_fields_default_none(self):
        req = DWHJoinRequestBase(**_base_kwargs())
        assert req.properties is None
        assert req.stats is None
        assert req.system is None

    def test_old_geospatial_attributes_field_removed(self):
        # Clean replacement: the legacy fields must no longer exist on the model.
        assert "geospatial_attributes" not in DWHJoinRequestBase.model_fields

    def test_old_attributes_field_removed(self):
        assert "attributes" not in DWHJoinRequestBase.model_fields

    def test_other_fields_unchanged(self):
        req = DWHJoinRequestBase(
            **_base_kwargs(
                with_geometry=False,
                output_format=OutputFormatEnum.GEOJSON,
                limit=100,
                offset=5,
                destination_crs=32632,
            )
        )
        assert req.with_geometry is False
        assert req.limit == 100
        assert req.offset == 5
        assert req.destination_crs == 32632
