"""Shared join DTOs for OGC API - Joins (extensions/joins/) and the future
tile-join quarantine (extensions/dwh/, untouched in this PR).

Phase 4b PR-1 ships only NamedSecondarySpec. PR-2 adds BigQuerySecondarySpec
with per-request target overrides + Secret-wrapped credentials.
"""

from __future__ import annotations

from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget


class NamedSecondarySpec(BaseModel):
    """Secondary identified by a registered collection id."""
    model_config = ConfigDict(extra="forbid")

    driver: Literal["registered"] = "registered"
    ref: str = Field(..., description="Collection id of the registered secondary.")


class BigQuerySecondarySpec(BaseModel):
    """Per-request BigQuery secondary — target identity supplied inline.

    Phase 4b PR-2: target only; auth still flows through CloudIdentityProtocol
    (same as Phase 4a's ``CollectionBigQueryDriver``). Phase 4e adds
    ``credentials: BigQueryCredentials`` for Secret-wrapped per-request
    credential overrides.
    """
    model_config = ConfigDict(extra="forbid")

    driver: Literal["bigquery"] = "bigquery"
    target: BigQueryTarget = Field(
        ..., description="Fully-qualified target (project_id, dataset_id, table_name).",
    )


SecondarySpec = Annotated[
    Union[NamedSecondarySpec, BigQuerySecondarySpec],
    Field(discriminator="driver"),
]


class JoinSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary_column: str = Field(..., description="Column name on the primary collection's features.")
    secondary_column: str = Field(..., description="Column name on the secondary's rows.")
    enrichment: bool = Field(
        default=True,
        description=(
            "If True, secondary columns are merged into the primary feature's "
            "properties (additive merge). If False, only matching feature ids "
            "are emitted with no property change."
        ),
    )


class ProjectionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    with_geometry: bool = True
    destination_crs: int = Field(default=4326, ge=1)
    attributes: Optional[List[str]] = Field(
        default=None, description="Subset of primary attributes to emit. None = all.",
    )


class PagingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)


class OutputSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["geojson", "json", "csv", "geopackage", "parquet"] = "geojson"
    encoding: str = "utf-8"


class JoinRequest(BaseModel):
    """Top-level POST body for /join/.../join.

    Raw SQL is NOT accepted — primary filter goes through structured
    filter DTOs; secondary side either references a registered
    collection (this PR) or supplies a typed BigQuerySecondarySpec (PR-2).
    """
    model_config = ConfigDict(extra="forbid")

    secondary: SecondarySpec
    join: JoinSpec
    projection: ProjectionSpec = Field(default_factory=ProjectionSpec)
    paging: Optional[PagingSpec] = None
    output: OutputSpec = Field(default_factory=OutputSpec)
