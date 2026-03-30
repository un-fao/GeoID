#    Copyright 2025 FAO
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

"""
Storage driver reference model.

``DriverRef`` is a validated reference to a named storage driver.
Import it from here or from ``dynastore.modules.catalog.catalog_config``.

Storage routing configuration (write_driver, read_drivers, secondary_drivers,
storage_locations) is now part of ``CollectionPluginConfig`` in
``dynastore.modules.catalog.catalog_config``.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DriverRef(BaseModel):
    """Validated reference to a registered storage driver."""

    model_config = ConfigDict(frozen=True)

    driver_id: str = Field(..., min_length=1)

    def __str__(self) -> str:
        return self.driver_id

    def __hash__(self) -> int:
        return hash(self.driver_id)

    @field_validator("driver_id")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("driver_id must be non-empty")
        return v
