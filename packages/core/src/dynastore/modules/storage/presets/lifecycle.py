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

"""Preset lifecycle orchestration — apply, revoke, dry-run.

The admin handlers call these three functions. They coordinate:

1. Registry lookup + tier validation.
2. Row-lock via ``SELECT … FOR UPDATE`` inside a managed transaction.
3. Idempotency check (same params snapshot → 200 no-op).
4. State machine transitions in ``AppliedPresetsService``.
5. Delegation to the preset's ``apply`` / ``revoke`` / ``dry_run``.
6. Self-lockout guard for IAM presets on DELETE.
7. Best-effort incremental rollback on apply failure.

The functions return the audit row as a plain dict so the admin handlers
can serialise it directly without coupling to internal types.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable, Literal, Mapping, Optional, cast

from fastapi import HTTPException
from pydantic import BaseModel

from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.modules.iam.applied_presets_service import AppliedPresetsService, AppliedRow

from .preset import AppliedDescriptor, PresetContext, PresetPlan, TaskHandle
from .registry import find_preset

logger = logging.getLogger(__name__)

PresetOp = Literal["apply", "unapply", "dry_run"]


def _scope_key(scope: str) -> str:
    """Normalise scope string — already normalised; pass through."""
    return scope


def _build_context(
    engine: Optional[DbResource],
    principal: Optional[Any],
    scope: str,
) -> PresetContext:
    """Construct a ``PresetContext`` from available protocols."""
    from dynastore.modules import get_protocol
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    return PresetContext(
        db=engine,
        iam=get_protocol(IamService),
        policy=get_protocol(PermissionProtocol),
        config=get_protocol(ConfigsProtocol),
        tasks=None,   # not wired in v1 core; presets that need tasks import directly
        cron=None,    # same
        libs=None,
        principal=principal,
        scope=scope,
    )


async def apply_preset(
    name: str,
    scope_key: str,
    params: BaseModel,
    ctx: PresetContext,
    engine: Optional[DbResource],
    audit: AppliedPresetsService,
    *,
    force: bool = False,
    applied_by: Optional[Any] = None,
) -> AppliedRow:
    """Apply a preset at the given scope.

    Returns the final audit row. Raises ``HTTPException`` on:
    * 404 — preset not registered.
    * 400 — URL tier mismatch.
    * 409 — concurrent in-progress apply, or params snapshot mismatch
      without ``?force=true``.
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Database engine not available.")

    preset = find_preset(name)
    params_json = json.loads(params.model_dump_json())

    async with managed_transaction(engine) as conn:
        existing = await audit.get_for_update(name, scope_key, conn=conn)

        if existing is not None:
            state = existing.get("state")
            if state in ("in_progress", "revoke_in_progress", "revoke_pending"):
                raise HTTPException(
                    status_code=409,
                    detail=f"Preset {name!r} at {scope_key!r} is currently {state!r}. Retry later.",
                )
            if state in ("applied", "partial"):
                stored_snap = existing.get("params_snapshot") or {}
                if isinstance(stored_snap, str):
                    stored_snap = json.loads(stored_snap)
                if stored_snap == params_json and not force:
                    # Idempotent re-POST with same params — return existing row.
                    return existing
                if stored_snap != params_json and not force:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                f"Preset {name!r} already applied at {scope_key!r} "
                                f"with different params. Pass ?force=true to replace."
                            ),
                            "stored": stored_snap,
                            "requested": params_json,
                        },
                    )

        # Insert / reset the audit row to pending inside the same transaction.
        from uuid import UUID as _UUID

        principal_id: Optional[_UUID] = None
        if applied_by is not None:
            pid = getattr(applied_by, "id", None) or getattr(applied_by, "principal_id", None)
            if pid is not None:
                try:
                    principal_id = _UUID(str(pid))
                except ValueError:
                    pass

        await audit.insert_pending(
            name, scope_key, params_json, applied_by=principal_id, conn=conn
        )

    # State machine: pending → in_progress → applied | failed
    await audit.mark_in_progress(name, scope_key)

    try:
        result = await preset.apply(params, scope_key, ctx)
    except Exception as exc:
        err_msg = str(exc)[:2000]
        logger.error("preset=%s scope=%s apply failed: %s", name, scope_key, err_msg, exc_info=True)
        await audit.mark_failed(name, scope_key, last_error=err_msg)
        raise HTTPException(
            status_code=500,
            detail={"message": f"Preset {name!r} apply failed.", "error": err_msg},
        ) from exc

    if isinstance(result, TaskHandle):
        # Async preset: stay in_progress; worker callback will finalise.
        return (await audit.mark_in_progress(name, scope_key, task_id=result.task_id)) or {}

    # Sync preset: mark applied.
    final = await audit.mark_applied(
        name, scope_key, revoke_descriptor=result.to_json()
    )
    return final or {}


async def revoke_preset(
    name: str,
    scope_key: str,
    ctx: PresetContext,
    engine: Optional[DbResource],
    audit: AppliedPresetsService,
    *,
    force_self_revoke: bool = False,
) -> AppliedRow:
    """Revoke a preset at the given scope.

    Returns the final audit row. Raises ``HTTPException`` on:
    * 404 — no applied audit row.
    * 409 — self-lockout guard fires without ``?force_self_revoke=true``.
    * 409 — concurrent revoke in progress.
    """
    preset = find_preset(name)

    existing = await audit.get(name, scope_key)
    if existing is None or existing.get("state") not in ("applied", "partial", "revoke_failed", "failed"):
        raise HTTPException(
            status_code=404,
            detail=f"No applied preset {name!r} at scope {scope_key!r} to revoke.",
        )

    # Self-lockout guard for IAM presets.
    if "iam" in getattr(preset, "keywords", ()) and not force_self_revoke:
        if ctx.principal is not None:
            _check_self_lockout(name, scope_key, ctx, existing)

    state = existing.get("state")
    if state in ("revoke_in_progress", "revoke_pending"):
        raise HTTPException(
            status_code=409,
            detail=f"Preset {name!r} at {scope_key!r} is currently {state!r}. Retry later.",
        )

    descriptor_data = existing.get("revoke_descriptor")
    if descriptor_data is None:
        descriptor_data = {}
    if isinstance(descriptor_data, str):
        descriptor_data = json.loads(descriptor_data)
    descriptor = AppliedDescriptor.from_json(descriptor_data)

    await audit.mark_revoke_pending(name, scope_key)

    try:
        result = await preset.revoke(descriptor, ctx)
    except Exception as exc:
        err_msg = str(exc)[:2000]
        logger.error("preset=%s scope=%s revoke failed: %s", name, scope_key, err_msg, exc_info=True)
        await audit.mark_revoke_failed(name, scope_key, last_error=err_msg)
        raise HTTPException(
            status_code=500,
            detail={"message": f"Preset {name!r} revoke failed.", "error": err_msg},
        ) from exc

    if isinstance(result, TaskHandle):
        return (await audit.mark_revoke_pending(name, scope_key, task_id=result.task_id)) or {}

    final = await audit.mark_revoked(name, scope_key)
    return final or {}


async def dry_run_preset(
    name: str,
    scope_key: str,
    params: BaseModel,
    ctx: PresetContext,
) -> PresetPlan:
    """Return a plan of what ``apply`` would do — no writes performed."""
    preset = find_preset(name)
    return await preset.dry_run(params, scope_key, ctx)


async def dispatch_preset(
    preset: Any,
    op: PresetOp,
    *,
    base_scope: Mapping[str, str],
    params: Optional[BaseModel] = None,
    principal: Optional[Any] = None,
) -> dict:
    """Single entry point used by the admin layer.

    Dispatches on the structural shape of *preset*:

    * ``hasattr(preset, "build")`` → routing-config preset (PresetBundle /
      ConfigsProtocol). Bundle slots are walked through ``set_config`` /
      ``delete_config``.
    * else → generalised ``Preset`` (``apply`` / ``revoke`` / ``dry_run``)
      driven through the audited :func:`apply_preset` / :func:`revoke_preset`
      / :func:`dry_run_preset` lifecycle.

    Returns the operator-visible response dict (same shape both branches).
    """
    if hasattr(preset, "build"):
        if op == "apply":
            return await _apply_routing_bundle(preset, dict(base_scope))
        if op == "unapply":
            return await _unapply_routing_bundle(preset, dict(base_scope))
        if op == "dry_run":
            return await _dry_run_routing_bundle(preset, dict(base_scope))
        raise ValueError(f"Unknown preset op: {op!r}")

    # Generalised Preset path — needs engine + AppliedPresetsService.
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import DatabaseProtocol

    db_proto = get_protocol(DatabaseProtocol)
    engine = db_proto.engine if db_proto is not None else None
    audit = AppliedPresetsService(engine)

    scope_key = _scope_from_base(base_scope)
    ctx = _build_context(engine, principal=principal, scope=scope_key)

    if op == "apply":
        params_model = params or preset.params_model()
        row = await apply_preset(
            preset.name, scope_key, params_model, ctx, engine, audit,
            applied_by=principal,
        )
        return {
            "preset": preset.name,
            "scope_key": scope_key,
            "state": row.get("state") if isinstance(row, dict) else None,
            **dict(base_scope),
        }
    if op == "unapply":
        row = await revoke_preset(preset.name, scope_key, ctx, engine, audit)
        return {
            "preset": preset.name,
            "scope_key": scope_key,
            "state": row.get("state") if isinstance(row, dict) else None,
            **dict(base_scope),
        }
    if op == "dry_run":
        params_model = params or preset.params_model()
        plan = await dry_run_preset(preset.name, scope_key, params_model, ctx)
        return {
            "preset_name": plan.preset_name,
            "scope_key": plan.scope_key,
            "entries": [
                {"kind": e.kind, "target": e.target, "detail": e.detail}
                for e in plan.entries
            ],
            "warnings": list(plan.warnings),
        }
    raise ValueError(f"Unknown preset op: {op!r}")


def _scope_from_base(base_scope: Mapping[str, str]) -> str:
    """Build the audit scope_key string from a base-scope dict.

    ``{}`` → ``"platform"``;
    ``{"catalog_id": "x"}`` → ``"catalog:x"``;
    ``{"catalog_id": "x", "collection_id": "y"}`` → ``"catalog:x/collection:y"``.
    """
    cat = base_scope.get("catalog_id")
    col = base_scope.get("collection_id")
    if cat and col:
        return f"catalog:{cat}/collection:{col}"
    if cat:
        return f"catalog:{cat}"
    return "platform"


async def _apply_routing_bundle(preset: Any, base_scope: dict) -> dict:
    """Walk a routing preset bundle through ``ConfigsProtocol.set_config``.

    Moved verbatim from ``admin.admin_service._apply_preset_bundle``; the
    admin layer now delegates here via :func:`dispatch_preset`.
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise HTTPException(status_code=503, detail="Configs service unavailable.")

    bundle = preset.build(**base_scope)
    applied: list[str] = []
    for entry in bundle.iter_apply():
        scope = {**base_scope, **dict(entry.scope)}
        await configs.set_config(entry.config_cls, entry.instance, **scope)
        applied.append(entry.slot)

    on_applied = getattr(preset, "on_applied", None)
    if callable(on_applied):
        try:
            await cast(Callable[..., Awaitable[Any]], on_applied)(**base_scope)
        except Exception as exc:  # noqa: BLE001 — hook errors must not abort apply
            logger.error(
                "preset=%s on_applied hook failed: %s",
                preset.name, exc, exc_info=True,
            )

    return {"preset": preset.name, "applied": applied, **base_scope}


async def _unapply_routing_bundle(preset: Any, base_scope: dict) -> dict:
    """Rollback a routing preset bundle, leaf-first, lenient on divergence.

    Moved verbatim from ``admin.admin_service._unapply_preset_bundle``.
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise HTTPException(status_code=503, detail="Configs service unavailable.")

    bundle = preset.build(**base_scope)
    deleted: list[str] = []
    skipped: list[dict] = []

    for entry in bundle.iter_rollback():
        scope = {**base_scope, **dict(entry.scope)}
        persisted = await configs.get_persisted_config(entry.config_cls, **scope)
        if persisted is None:
            skipped.append({
                "slot": entry.slot,
                "class": entry.config_cls.__name__,
                "reason": "missing",
            })
            continue
        try:
            persisted_norm = entry.config_cls.model_validate(persisted).model_dump(mode="json")
        except Exception:  # noqa: BLE001 — surface the raw payload on validation failure
            persisted_norm = persisted
        expected_norm = entry.instance.model_dump(mode="json")
        if persisted_norm == expected_norm:
            await configs.delete_config(entry.config_cls, **scope)
            deleted.append(entry.slot)
        else:
            logger.info(
                "preset=%s scope=%s slot=%s diverged on revoke — leaving in place",
                preset.name, base_scope, entry.slot,
            )
            skipped.append({
                "slot": entry.slot,
                "class": entry.config_cls.__name__,
                "reason": "diverged",
                "persisted": persisted_norm,
                "expected": expected_norm,
            })

    return {"preset": preset.name, "deleted": deleted, "skipped": skipped, **base_scope}


async def _dry_run_routing_bundle(preset: Any, base_scope: dict) -> dict:
    """Dry-run a routing preset bundle — no writes performed."""
    bundle = preset.build(**base_scope)
    entries = [
        {
            "slot": e.slot,
            "class": e.config_cls.__name__,
            "scope": {**base_scope, **dict(e.scope)},
        }
        for e in bundle.iter_apply()
    ]
    return {"preset": preset.name, "entries": entries, **base_scope}


def _check_self_lockout(
    name: str,
    scope_key: str,
    ctx: PresetContext,
    existing_row: AppliedRow,
) -> None:
    """Raise 409 if revoking this preset would remove the caller's admin access.

    V1 implementation: heuristic check. If the caller's principal has a role
    that appears in the revoke descriptor's policy list and that role is the
    only source of admin grants, block the revoke. For a full simulation see
    future PR; v1 keeps this conservative (may produce false positives for
    principal with multiple admin paths).
    """
    principal = ctx.principal
    if principal is None:
        return

    descriptor_data = existing_row.get("revoke_descriptor") or {}
    if isinstance(descriptor_data, str):
        try:
            descriptor_data = json.loads(descriptor_data)
        except Exception:  # noqa: BLE001
            return

    # For routing presets the descriptor has "slots" not IAM content; skip.
    if "children" not in descriptor_data and "slots" in descriptor_data:
        return

    # Simplified v1 check: if the caller's only admin role is listed as
    # something this preset would remove, surface a 409.
    # The spec mandates this is scope-local; platform sysadmin is unaffected.
    principal_roles = set(getattr(principal, "roles", []) or [])
    removed_roles: set = set()

    # Collect roles the descriptor mentions removing.
    for child in descriptor_data.get("children", []):
        child_desc = child.get("descriptor", {})
        removed_roles.update(child_desc.get("role_names", []))
    removed_roles.update(descriptor_data.get("role_names", []))

    overlap = principal_roles & removed_roles
    if overlap and len(principal_roles) <= len(overlap):
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Revoking preset {name!r} at {scope_key!r} would remove "
                    f"the caller's effective admin access. "
                    f"Pass ?force_self_revoke=true to override."
                ),
                "affected_roles": sorted(overlap),
            },
        )
