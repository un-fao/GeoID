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

"""``public_access_baseline`` preset — minimal anon-allow Policy for headless infra.

Closes the chicken-and-egg gap from #1412: every extension policy is
operator-applied, but Cloud Run /health startup probe runs anonymously
before any operator can log in.  This preset upserts a single
``public_access`` Policy and unions it into the ``unauthenticated`` role.

IamModule.lifespan applies this once per DB via iam.applied_presets
sentinel on cold-boot so the probe never 403s.  Restarts no-op.
Sysadmin DELETE of the preset (and its sentinel row) is a deliberate choice
to disable anonymous discovery.  Carries ``destructive_if_revoked`` keyword
so the UI surfaces a confirmation gate.
"""
from __future__ import annotations

import logging
from typing import ClassVar, List, Tuple, Type

from pydantic import BaseModel

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)

_POLICY_ID = "public_access"
_ROLE_NAME = "unauthenticated"

_PUBLIC_ACCESS_POLICY = Policy(
    id=_POLICY_ID,
    description=(
        "Anonymous access to liveness probe, OpenAPI doc, and .well-known "
        "discovery. Required for headless infrastructure probes "
        "(e.g. Cloud Run startup probe)."
    ),
    actions=["GET", "OPTIONS"],
    resources=["/health$", "/openapi.json", "/.well-known/.*"],
    effect="ALLOW",
    priority=0,
)


class PublicAccessBaseline:
    """Minimal anon-allow Policy for headless infra probes.

    Upserts ``public_access`` Policy and unions it into the
    ``unauthenticated`` role.  DELETE removes only the policy this preset
    wrote and strips ``public_access`` from the role; the role itself is
    preserved.
    """

    name: ClassVar[str] = "public_access_baseline"
    description: ClassVar[str] = (
        "Minimal anon-allow Policy for headless infra probes (/health, "
        "/openapi.json, /.well-known). Required default; sysadmin can DELETE "
        "if they explicitly want to break unauthenticated discovery."
    )
    keywords: ClassVar[Tuple[str, ...]] = (
        "iam", "platform", "foundational", "infra", "destructive_if_revoked"
    )
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    is_async: ClassVar[bool] = False

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        entries: List[PresetPlanEntry] = [
            PresetPlanEntry(
                kind="upsert_policy",
                target=_POLICY_ID,
                detail={
                    "actions": _PUBLIC_ACCESS_POLICY.actions,
                    "resources": _PUBLIC_ACCESS_POLICY.resources,
                    "effect": _PUBLIC_ACCESS_POLICY.effect,
                },
            ),
            PresetPlanEntry(
                kind="update_role_binding",
                target=_ROLE_NAME,
                detail={"add_policies": [_POLICY_ID]},
            ),
        ]
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        # Upsert the policy — update first, create if absent.
        updated = await ctx.policy.update_policy(_PUBLIC_ACCESS_POLICY)
        if updated is None:
            await ctx.policy.create_policy(_PUBLIC_ACCESS_POLICY)
        logger.debug("public_access_baseline: upserted policy %s", _POLICY_ID)

        # Union 'public_access' into unauthenticated role (read-modify-write).
        await _union_policy_into_role(ctx.iam, _ROLE_NAME, _POLICY_ID)

        return AppliedDescriptor(
            payload={
                "preset_name": self.name,
                "policy_ids": [_POLICY_ID],
                "role_names": [_ROLE_NAME],
            }
        )

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        # Strip 'public_access' from unauthenticated role; preserve other policies.
        await _strip_policy_from_role(ctx.iam, _ROLE_NAME, _POLICY_ID)

        # Delete only the policy this preset wrote.
        await ctx.policy.delete_policy(_POLICY_ID)
        logger.debug(
            "public_access_baseline: revoked policy %s and stripped from role %s",
            _POLICY_ID, _ROLE_NAME,
        )
        return None


async def _get_role_by_name(iam: object, role_name: str) -> "Role | None":
    """Fetch a role by name via the IamService list_roles API."""
    roles: List[Role] = await iam.list_roles()  # type: ignore[union-attr]
    return next((r for r in roles if r.name == role_name), None)


async def _union_policy_into_role(iam: object, role_name: str, policy_id: str) -> None:
    """Add ``policy_id`` to ``role_name``'s policy list if not already present."""
    existing = await _get_role_by_name(iam, role_name)
    if existing is None:
        seed = Role(name=role_name, policies=[policy_id])
        await iam.create_role(seed)  # type: ignore[union-attr]
        return
    if policy_id not in existing.policies:
        merged_policies = list(existing.policies) + [policy_id]
        merged = existing.model_copy(update={"policies": merged_policies})
        await iam.update_role(merged)  # type: ignore[union-attr]


async def _strip_policy_from_role(iam: object, role_name: str, policy_id: str) -> None:
    """Remove ``policy_id`` from ``role_name``'s policy list; leave role intact."""
    existing = await _get_role_by_name(iam, role_name)
    if existing is None:
        return
    remaining = [p for p in existing.policies if p != policy_id]
    updated = existing.model_copy(update={"policies": remaining})
    await iam.update_role(updated)  # type: ignore[union-attr]
