"""BigQuery driver DTOs (Phase 4a — READ path only).

Per-request credential overrides + Secret-wrapped credentials land in
Phase 4b/4e. This phase uses CloudIdentityProtocol for all auth, so the
driver config only carries target identity.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.storage.driver_config import CollectionDriverConfig
from dynastore.tools.secrets import Secret


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


class BigQueryCredentials(BaseModel):
    """Secret-wrapped BigQuery credentials.

    Registered-per-collection only — stored encrypted at rest in the
    platform's PluginConfig jsonb, masked in API responses, revealed only
    inside the BQ client constructor. Matches the platform credential
    framework (see project_credential_framework.md memory note).

    Per-request credential overrides (spec lines 546-581's
    BigQuerySecondarySpec with credentials field) are explicitly NOT
    added in this phase per user direction.
    """

    model_config = ConfigDict(extra="forbid")

    service_account_json: Optional[Secret] = Field(
        default=None,
        description="Full service-account JSON (Secret-wrapped). Preferred.",
    )
    api_key: Optional[Secret] = Field(
        default=None,
        description="BigQuery External Connections API key (Secret-wrapped). Future-proof.",
    )

    def is_empty(self) -> bool:
        """True iff no credential material supplied.

        Drivers fall back to CloudIdentityProtocol (Phase 4a path) when
        ``is_empty()`` — preserves back-compat for deployments that never
        migrate to Secret-wrapped credentials.
        """
        return self.service_account_json is None and self.api_key is None


class ItemsBigQueryDriverConfig(CollectionDriverConfig):
    """Registered per-collection config for the BigQuery driver.

    Phase 4a: READ-only, credentials via CloudIdentityProtocol.
    Phase 4b adds ``credentials: BigQueryCredentials`` for per-request overrides.
    Phase 4d adds write-path fields (streaming_threshold, partition_column, etc.).
    Phase 4e adds registered-per-collection Secret-wrapped credentials on the
    config itself; per-request overrides remain deferred.
    """

    target: BigQueryTarget = Field(default_factory=BigQueryTarget)
    credentials: BigQueryCredentials = Field(default_factory=BigQueryCredentials)
    location: str = "EU"
    page_size: int = Field(default=1000, ge=1, le=50000)
    query_timeout_s: int = Field(default=60, ge=1, le=600)


# ---------------------------------------------------------------------------
# Back-compat aliases — legacy Collection*DriverConfig names remain importable, and
# registry lookups (driver_index / TypedModelRegistry) go through the
# config_rewriter so persisted routing entries and config rows still resolve.
# Remove once telemetry shows zero hits on the rewriter.  See
# dynastore.modules.db_config.config_rewriter.
# ---------------------------------------------------------------------------
from dynastore.modules.db_config.config_rewriter import register_config_class_key_rename  # noqa: E402

CollectionBigQueryDriverConfig = ItemsBigQueryDriverConfig  # noqa: E305 — back-compat alias, see config_rewriter
register_config_class_key_rename(
    legacy="CollectionBigQueryDriverConfig",
    canonical="ItemsBigQueryDriverConfig",
)
