"""BigQuery driver DTOs (Phase 4a — READ path only).

Per-request credential overrides + Secret-wrapped credentials land in
Phase 4b/4e. This phase uses CloudIdentityProtocol for all auth, so the
driver config only carries target identity.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.storage.driver_config import CollectionDriverConfig


class BigQueryTarget(BaseModel):
    """Identity of a BQ read target. All fields optional so Phase 4b's
    per-request overrides can supply them later; Phase 4a requires all
    three to be present on the registered config."""

    model_config = ConfigDict(extra="forbid")

    project_id: Optional[str] = None
    dataset_id: Optional[str] = None
    table_name: Optional[str] = None

    def is_fully_qualified(self) -> bool:
        return bool(self.project_id and self.dataset_id and self.table_name)

    def fqn(self) -> str:
        if not self.is_fully_qualified():
            raise ValueError(
                "BigQueryTarget is not fully qualified; "
                "all of project_id, dataset_id, table_name required",
            )
        return f"{self.project_id}.{self.dataset_id}.{self.table_name}"


class CollectionBigQueryDriverConfig(CollectionDriverConfig):
    """Registered per-collection config for the BigQuery driver.

    Phase 4a: READ-only, credentials via CloudIdentityProtocol.
    Phase 4b adds ``credentials: BigQueryCredentials`` for per-request overrides.
    Phase 4d adds write-path fields (streaming_threshold, partition_column, etc.).
    """

    target: BigQueryTarget = Field(default_factory=BigQueryTarget)
    location: str = "EU"
    page_size: int = Field(default=1000, ge=1, le=50000)
    query_timeout_s: int = Field(default=60, ge=1, le=600)
