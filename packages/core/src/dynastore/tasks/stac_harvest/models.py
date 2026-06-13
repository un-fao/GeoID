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

"""Input model for the stac_harvest OGC Process."""

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class StacHarvestRequest(BaseModel):
    """Inputs for the ``stac_harvest`` OGC Process.

    Harvests a remote STAC catalog (``catalog_url``) into a local dynastore
    catalog (``target_catalog``).  Collections and items are upserted
    idempotently, keyed on the STAC ``id`` field.
    """

    catalog_url: str = Field(
        ...,
        description=(
            "Base URL of the source STAC catalog — must expose "
            "/collections and /collections/{id}/items."
        ),
    )
    target_catalog: str = Field(
        ...,
        description="ID of the local dynastore catalog to write into.",
    )
    max_collections: int = Field(
        default=0,
        ge=0,
        description="Maximum number of source collections to harvest (0 = all).",
    )
    max_items: int = Field(
        default=0,
        ge=0,
        description="Maximum number of items per collection to harvest (0 = all).",
    )
    with_assets: bool = Field(
        default=True,
        description=(
            "When True, register each item asset href as a virtual asset "
            "(dynastore stores only the href, never the bytes)."
        ),
    )
    storage_backend: Literal["es", "es_pg", "pg"] = Field(
        default="es",
        description=(
            "Item storage backend for this harvest.  "
            "``es`` routes item WRITE and READ directly to Elasticsearch so "
            "harvested items are immediately searchable without waiting for the "
            "async ES-index drain.  ``es_pg`` writes to PG primary with an async "
            "ES secondary index (default platform routing).  ``pg`` uses PG only.  "
            "This field takes precedence over the legacy ``es_only`` flag when both "
            "are supplied."
        ),
    )
    es_only: Optional[bool] = Field(
        default=None,
        description=(
            "Legacy flag — prefer ``storage_backend``.  When set and "
            "``storage_backend`` is not explicitly provided: ``True`` maps to "
            "``storage_backend='es'``; ``False`` maps to ``storage_backend='es_pg'``.  "
            "Ignored when ``storage_backend`` is given explicitly."
        ),
    )

    @field_validator("catalog_url")
    @classmethod
    def _validate_catalog_url(cls, v: str) -> str:
        if not v.startswith("https://") and not v.startswith("http://"):
            raise ValueError("catalog_url must start with http:// or https://")
        return v.rstrip("/")

    @model_validator(mode="after")
    def _resolve_backend(self) -> "StacHarvestRequest":
        """Map legacy ``es_only`` → ``storage_backend`` when the caller did not
        supply ``storage_backend`` explicitly.  ``storage_backend`` always wins
        when both are given."""
        if self.es_only is not None and self.storage_backend == "es":
            # ``storage_backend`` is still at its default value; apply the
            # es_only mapping so the caller's intent is preserved.
            self.storage_backend = "es" if self.es_only else "es_pg"
        return self
