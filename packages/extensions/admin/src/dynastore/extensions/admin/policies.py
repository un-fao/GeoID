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

"""Pure declarations of admin-extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through ``AdminService.get_policies`` / ``get_role_bindings`` and
forwards them to ``PermissionProtocol``. This file never calls
``register_policy`` directly — see PR #308 for the architectural rule.
"""

from typing import List, Optional
from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def admin_policies() -> List[Policy]:
    """Pure declaration of the admin extension's policies.

    Three policies:

    - ``admin_access`` — broad ``/admin/.*`` ALLOW. Default role
      bindings (returned by ``admin_role_bindings``) carry SYSADMIN +
      ADMIN for back-compat with existing deployments; operators may
      add/remove via REST.

    - ``admin_catalog_access`` — per-catalog mutation surface gated by
      ``catalog_admin_required``. Ships with the configured admin role
      (``IamRolesConfig().admin_role_name``) as the default delegation
      role so catalog-admin delegation works out of the box; operators
      override the list via the ``iam_baseline`` preset's
      ``delegation_role_names`` or by editing the policy's condition.
      An empty ``required_roles`` list is treated as deny-all by the
      condition handler.

    - ``admin_catalogs_list`` — narrow ALLOW for ``GET /admin/catalogs``
      (the catalog picker, #723). Bound by default to the
      ``catalog_admin`` sentinel role (auto-added by ``IamMiddleware``
      to any principal who holds the catalog-tier admin role) so
      catalog admins can list catalogs they administer.  The handler
      filters the response per caller; sysadmin / platform-admin reach
      this route via ``admin_access`` and get the unfiltered list.

    - ``admin_principal_lookup`` — narrow ALLOW for ``GET /admin/principals``
      (#723 follow-up). Bound to the ``catalog_admin`` sentinel so the
      catalog-grant flow can resolve a target principal by subject_id
      before calling ``POST /admin/catalogs/{cid}/principals/{pid}/roles``.
      The handler requires a non-empty ``q`` parameter from catalog-only
      callers so they cannot enumerate the platform principal directory.

    - ``admin_task_dispatch`` — POST on the catalog-level task dispatch
      surface (``/admin/catalogs/{cat}/tasks``). Bound to sysadmin +
      admin so both privileged platform roles can trigger tasks at the
      catalog scope (e.g. reindex). The path pattern uses a regex anchor
      so it does not match the collection-scoped variant, which is
      covered by the separate ``admin_task_dispatch_collection`` policy.

    - ``admin_task_dispatch_collection`` — POST on the collection-level
      task dispatch surface
      (``/admin/catalogs/{cat}/collections/{col}/tasks``). Bound to
      sysadmin + admin so a catalog-admin who can reindex a whole
      catalog can also act on a single collection within it (the
      collection scope is a subset of the catalog scope — gating it more
      tightly than the catalog route would be inconsistent).

    - ``admin_task_dispatch_privileged_deny`` — DENY policy that restores
      per-action privilege gating on both task-dispatch surfaces. Using
      the ``request_action_privilege`` condition handler it denies any
      principal that does NOT hold the sysadmin role when the request body
      ``action`` is in the gated set (currently ``backfill_envelope_attrs``).
      Sysadmin principals are exempt via the handler's ``allow_sysadmin``
      bypass, so they can trigger all actions.  Admin principals are
      allowed to trigger non-gated actions (e.g. ``reindex``) because the
      condition returns ``False`` (DENY does not apply) for those.  The
      policy is bound to BOTH sysadmin and admin so it appears in each
      principal's effective policy set; the condition itself differentiates
      between them at evaluation time.
    """
    return [
        Policy(
            id="admin_access",
            description="Grants full access to admin management endpoints.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/admin",
                "/admin/",
                "/admin/.*",
                "/web/pages/admin_panel",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="admin_catalog_access",
            description=(
                "Per-catalog admin access; admits roles declared in this "
                "policy's catalog_admin_required condition. Sysadmin and "
                "platform-grant principals bypass."
            ),
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    # Seed the configured catalog-admin role as the default so
                    # catalog-admin delegation works out of the box and survives
                    # a process restart (this contributor re-runs on every boot).
                    # iam_baseline may override with operator-configured
                    # delegation roles; the default must never be the empty list,
                    # which catalog_admin_required treats as deny-all.
                    config={"required_roles": [IamRolesConfig().admin_role_name]},
                )
            ],
        ),
        Policy(
            id="admin_catalogs_list",
            description=(
                "Lets catalog-tier admins reach GET /admin/catalogs. The "
                "handler filters the response so each catalog admin only "
                "sees the catalogs they actually administer; sysadmin / "
                "platform-admin reach this route via admin_access and "
                "see the full list."
            ),
            actions=["GET"],
            resources=[r"^/admin/catalogs/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_principal_lookup",
            description=(
                "Lets catalog-tier admins reach GET /admin/principals to "
                "resolve a target principal by subject_id before granting a "
                "catalog-scope role. The handler requires a non-empty q "
                "parameter from catalog-only callers so they cannot "
                "enumerate the platform principal directory."
            ),
            actions=["GET"],
            resources=[r"^/admin/principals/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_task_dispatch",
            description=(
                "Grants admin and sysadmin the ability to dispatch tasks at "
                "catalog scope via POST /admin/catalogs/{cat}/tasks."
            ),
            actions=["POST"],
            resources=[r"^/admin/catalogs/[^/]+/tasks$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_task_dispatch_collection",
            description=(
                "Grants admin and sysadmin the ability to dispatch tasks at "
                "collection scope via POST "
                "/admin/catalogs/{cat}/collections/{col}/tasks. Collection "
                "scope is a subset of the catalog route, so it carries the "
                "same admin+sysadmin binding."
            ),
            actions=["POST"],
            resources=[r"^/admin/catalogs/[^/]+/collections/[^/]+/tasks$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_task_dispatch_privileged_deny",
            description=(
                "Denies privileged task actions (e.g. backfill_envelope_attrs) "
                "to principals who do not hold the sysadmin role. Applies to "
                "both the catalog-scope and collection-scope task-dispatch "
                "surfaces. The request_action_privilege condition reads the "
                "POST body action field and only triggers the DENY when the "
                "action is in the gated set AND the principal lacks sysadmin. "
                "Admin principals may still trigger non-gated actions (e.g. "
                "reindex) because the condition returns False for those."
            ),
            actions=["POST"],
            resources=[
                r"^/admin/catalogs/[^/]+/tasks$",
                r"^/admin/catalogs/[^/]+/collections/[^/]+/tasks$",
            ],
            effect="DENY",
            conditions=[
                Condition(
                    type="request_action_privilege",
                    config={
                        "gated_actions": ["backfill_envelope_attrs"],
                        "required_role": IamRolesConfig().sysadmin_role_name,
                    },
                )
            ],
        ),
    ]


def admin_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the admin extension's role bindings.

    - ``admin_access`` is bound to sysadmin + admin (back-compat).
    - ``admin_catalogs_list`` + ``admin_principal_lookup`` are bound to the
      ``catalog_admin`` sentinel role so catalog-tier admins reach the
      catalog picker (#723) and the target-user lookup that precedes a
      catalog-scope role grant (#723 follow-up).
    - ``admin_catalog_access`` is intentionally unbound — operators wire
      catalog-admin authority through the same DB-resident flow they
      use for any custom role.
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        Role(name=sysadmin_role_name, policies=["admin_access"]),
        Role(name=admin_role_name, policies=["admin_access"]),
        Role(
            name="catalog_admin",
            policies=["admin_catalogs_list", "admin_principal_lookup"],
        ),
        # Task-dispatch: both admin tiers can trigger catalog- and
        # collection-scoped tasks (e.g. reindex). The collection scope is a
        # subset of the catalog scope, so it carries the same admin+sysadmin
        # binding rather than a tighter one.
        Role(name=sysadmin_role_name, policies=["admin_task_dispatch"]),
        Role(name=admin_role_name, policies=["admin_task_dispatch"]),
        Role(name=sysadmin_role_name, policies=["admin_task_dispatch_collection"]),
        Role(name=admin_role_name, policies=["admin_task_dispatch_collection"]),
        # Per-action privilege gate: the DENY policy must appear in the
        # effective policy set of BOTH sysadmin and admin so it is evaluated.
        # The request_action_privilege condition exempts sysadmin at evaluation
        # time, so binding sysadmin here is harmless and keeps coverage symmetric.
        Role(name=sysadmin_role_name, policies=["admin_task_dispatch_privileged_deny"]),
        Role(name=admin_role_name, policies=["admin_task_dispatch_privileged_deny"]),
    ]
