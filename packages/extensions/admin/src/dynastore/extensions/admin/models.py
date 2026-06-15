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

from datetime import datetime
from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field


# --- Principal request DTOs ---
# Response shape is `PrincipalResponse` in dynastore.models.protocols.policies.

class PrincipalCreate(BaseModel):
    username: str
    password: Optional[str] = None
    email: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    provider: str = "local"
    subject_id: Optional[str] = None


class PrincipalUpdate(BaseModel):
    email: Optional[str] = None
    is_active: Optional[bool] = None
    roles: Optional[List[str]] = None


# --- Role / Policy wire DTOs live in dynastore.models.protocols.policies. ---


class UsageRow(BaseModel):
    """One row of usage-counter state returned by ``GET /admin/policies/{id}/usage``."""

    principal_key: str
    count: int
    window_start: str
    expires_at: Optional[str] = None
    last_seen_at: Optional[str] = None


class UsagePage(BaseModel):
    """Paged listing of usage-counter rows for a given policy."""

    policy_id: str
    rows: List[UsageRow]
    next_offset: Optional[int] = None


class UsageResetResponse(BaseModel):
    policy_id: str
    principal_key: str
    reset_count: int


class GrantRateLimitCounter(BaseModel):
    """Live state for a rate-limit condition on one binding."""

    count: int = Field(description="Current hits in the active window.")
    limit: int = Field(description="Max hits allowed per window (from grant.quota).")
    window_seconds: int = Field(
        description="Window width in seconds — counter rolls every N seconds.",
    )
    remaining: int = Field(
        description="``max(0, limit - count)`` — convenience for the UI.",
    )
    window_start: Optional[str] = Field(
        default=None,
        description=(
            "ISO 8601 timestamp of the current bucket's start, derived from "
            "``bucket_for(window_seconds)``. ``None`` only when the counter "
            "shape doesn't carry a window."
        ),
    )


class GrantMaxCountCounter(BaseModel):
    """Live state for a lifetime-quota (``max_count``) condition on one binding."""

    count: int = Field(description="Lifetime accumulated hits (never resets unless reset).")
    limit: int = Field(description="Max lifetime hits allowed (from grant.quota).")
    remaining: int = Field(description="``max(0, limit - count)``.")


class GrantUsageCounters(BaseModel):
    """The pair of counter views attached to a binding (either may be None)."""

    rate_limit: Optional[GrantRateLimitCounter] = None
    max_count: Optional[GrantMaxCountCounter] = None


class GrantUsageEntry(BaseModel):
    """One binding row and its live counter state."""

    grant_id: str
    subject_kind: str = Field(description="``principal`` (direct grant) or ``role``.")
    subject_ref: str = Field(description="Principal id or role name on the binding's subject side.")
    object_kind: str = Field(description="``role`` or ``policy`` — what's bound.")
    object_ref: str = Field(description="Role name or policy id.")
    effect: str = Field(description="``allow`` or ``deny``.")
    resource_kind: Optional[str] = Field(
        default=None,
        description="``collection`` / ``item`` / ... when scoped to a resource.",
    )
    resource_ref: Optional[str] = None
    quota_spec: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Echo of the grant's ``quota`` JSONB. ``None`` when the binding "
            "carries no per-binding quota — counters will then reflect any "
            "platform-default rate_limit / max_count from "
            ":class:`IamScaleConfig`."
        ),
    )
    counters: GrantUsageCounters = Field(
        default_factory=GrantUsageCounters,
        description="Live counter state for this binding.",
    )
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None


class GrantUsageView(BaseModel):
    """Wire response for ``GET /admin/iam/usage/grants``."""

    principal_id: str
    catalog_id: Optional[str] = None
    entries: List[GrantUsageEntry] = Field(default_factory=list)
    valkey_available: bool = Field(
        description=(
            "False when the Valkey hot tier was unreachable; figures may be "
            "stale (served from the PG durability tier) but are still "
            "valid for inspection. True when the live Valkey counters "
            "were the source of every entry."
        ),
    )
    fetched_at: str = Field(description="ISO 8601 timestamp the response was assembled.")


# --- Admin task-dispatch request / response DTOs ---


class AdminTaskRequest(BaseModel):
    """Request body for POST /admin/catalogs/{cat}/tasks and
    POST /admin/catalogs/{cat}/collections/{col}/tasks."""

    action: str = Field(description="Action to enqueue (e.g. 'reindex', 'backfill_envelope_attrs').")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Action-specific parameters (optional).",
    )


class AdminTaskTarget(BaseModel):
    """Identifies the scope the dispatched task targets."""

    catalog_id: str
    collection_id: Optional[str] = None


class AdminTaskResponse(BaseModel):
    """202 Accepted response for admin task-dispatch endpoints."""

    task_id: str
    action: str
    target: AdminTaskTarget
    status: str = "queued"


# --- ES-vs-PG index-drift report DTOs (#2044) ---


class CollectionIndexDrift(BaseModel):
    """Per-collection PostgreSQL (source-of-truth) vs Elasticsearch counts."""

    collection_id: str
    es_active: bool = Field(
        description="Whether the regular ES driver is in this collection's routing config."
    )
    pg_count: int = Field(
        description="Authoritative item count from the PostgreSQL source-of-truth read driver."
    )
    es_count: int = Field(
        description="Document count in the Elasticsearch items index for this collection."
    )
    drift: int = Field(
        description=(
            "pg_count - es_count. Positive means ES is missing documents that "
            "PG holds (e.g. un-drained async writes or geo_shape-rejected docs)."
        )
    )
    in_sync: bool = Field(
        description="True when ES matches PG, or when ES is not active for this collection."
    )


class IndexDriftReport(BaseModel):
    """ES-vs-PG item-index drift report for a catalog (read-only diagnostic).

    Remediation for a non-zero drift is the existing reindex-from-PG task:
    POST /admin/catalogs/{cat}/tasks {"action": "reindex"} (or the
    collection-scoped variant), which rebuilds the ES index from PG.
    """

    catalog_id: str
    collections: List[CollectionIndexDrift]
    total_pg: int = Field(description="Sum of pg_count across ES-active collections.")
    total_es: int = Field(description="Sum of es_count across ES-active collections.")
    total_drift: int = Field(
        description="total_pg - total_es across ES-active collections."
    )
    in_sync: bool
