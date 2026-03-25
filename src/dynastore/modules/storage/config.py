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
Storage Routing Configuration.

Defines which driver(s) to use for a collection, leveraging the existing
4-tier ``ConfigsProtocol`` hierarchy (collection > catalog > platform > defaults).

All fields are strongly typed Pydantic models with auto-coercion from
shorthand strings for backward compatibility with existing JSON configs.
"""

from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.modules.storage.location import (
    StorageLocationConfig,
    StorageLocationConfigRegistry,
)

STORAGE_ROUTING_CONFIG_ID = "storage_routing"


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


class StorageRoutingConfig(PluginConfig):
    """Typed, extensible routing config for multi-driver storage.

    All fields are strongly typed Pydantic models. Storage locations
    are deserialized via ``StorageLocationConfigRegistry`` for per-driver
    typed validation.

    Attributes:
        primary_driver: Driver for writes and default reads.
        read_drivers: Hint-to-driver mapping for reads. Keys are
            ``ReadHint`` values or custom hint strings.
        secondary_drivers: Drivers receiving async write fan-out
            via ``EventsProtocol``.
        storage_locations: Per-driver storage location config.
            Deserialized via registry for typed validation.
    """

    _plugin_id: ClassVar[Optional[str]] = STORAGE_ROUTING_CONFIG_ID

    primary_driver: DriverRef = Field(
        default_factory=lambda: DriverRef(driver_id="postgresql"),
        description="Driver for writes and default reads.",
    )
    read_drivers: Dict[str, DriverRef] = Field(
        default_factory=dict,
        description="Hint -> driver mapping for reads.",
    )
    secondary_drivers: List[DriverRef] = Field(
        default_factory=list,
        description="Drivers receiving async write fan-out via EventsProtocol.",
    )
    storage_locations: Dict[str, StorageLocationConfig] = Field(
        default_factory=dict,
        description="Per-driver storage location config.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_driver_refs(cls, data: Any) -> Any:
        """Allow shorthand strings: ``'postgresql'`` -> ``DriverRef(driver_id='postgresql')``."""
        if not isinstance(data, dict):
            return data

        pd = data.get("primary_driver")
        if isinstance(pd, str):
            data["primary_driver"] = {"driver_id": pd}

        rd = data.get("read_drivers")
        if isinstance(rd, dict):
            data["read_drivers"] = {
                k: {"driver_id": v} if isinstance(v, str) else v
                for k, v in rd.items()
            }

        sd = data.get("secondary_drivers")
        if isinstance(sd, list):
            data["secondary_drivers"] = [
                {"driver_id": s} if isinstance(s, str) else s for s in sd
            ]

        sl = data.get("storage_locations")
        if isinstance(sl, dict):
            resolved = {}
            for driver_id, loc_data in sl.items():
                if isinstance(loc_data, dict):
                    config_cls = StorageLocationConfigRegistry.resolve(driver_id)
                    resolved[driver_id] = config_cls.model_validate(loc_data)
                else:
                    resolved[driver_id] = loc_data
            data["storage_locations"] = resolved

        return data

    def get_location(self, driver_id: str) -> Optional[StorageLocationConfig]:
        """Get the storage location config for a driver."""
        return self.storage_locations.get(driver_id)

    def available_hints(self) -> Set[str]:
        """Return all configured hint keys."""
        return set(self.read_drivers.keys())

    @property
    def primary_driver_id(self) -> str:
        """Shortcut: primary driver ID as string."""
        return self.primary_driver.driver_id

    def resolve_read_driver_id(self, hint: str) -> str:
        """Resolve the driver ID for a read hint, with fallback chain."""
        ref = self.read_drivers.get(hint) or self.read_drivers.get("default")
        return ref.driver_id if ref else self.primary_driver_id

    @property
    def secondary_driver_ids(self) -> List[str]:
        """Shortcut: secondary driver IDs as strings."""
        return [d.driver_id for d in self.secondary_drivers]
