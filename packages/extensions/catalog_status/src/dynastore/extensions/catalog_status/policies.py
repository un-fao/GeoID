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

"""Pure declarations of catalog_status extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through the preset registered in ``presets/__init__.py`` and forwards them
to ``PermissionProtocol``.  This file never calls ``register_policy``
directly.

Two concerns:

``catalog_status_read``
    ALLOW ``GET`` on catalog/collection status views.  The 404-on-hidden
    enforcement in the route handler is what protects information about
    hidden catalogs; the policy just needs to be broad enough that any
    authenticated caller (or anonymous, if the platform is open) can reach
    the route.  Binding mirrors how the STAC and OGC extensions open their
    per-catalog read surfaces.

``catalog_status_admin``
    ALLOW mutations (reprovision, dead-letter list/requeue) for
    catalog-admin-tier and above, gated by the ``catalog_admin_required``
    condition.  Policy id and shape mirror ``admin_catalog_access``.
"""
from typing import List, Optional

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def catalog_status_policies() -> List[Policy]:
    """Pure declaration of the catalog_status extension's policies."""
    return [
        Policy(
            id="catalog_status_read",
            description=(
                "Allows any caller to reach the catalog/collection status "
                "read surfaces (GET /catalog/catalogs/{id} and "
                "GET /catalog/catalogs/{id}/collections/{col}). "
                "Visibility-gating (404 for hidden catalogs) is enforced in "
                "the route handler, not here."
            ),
            actions=["GET"],
            resources=[
                r"^/catalog/catalogs/[^/]+(/collections/[^/]+)?$",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="catalog_status_admin",
            description=(
                "Per-catalog admin access for catalog_status mutation surfaces "
                "(reprovision, dead-letter list and requeue). Gated by the "
                "catalog_admin_required condition so only catalog-tier admins "
                "and above may trigger recovery operations."
            ),
            actions=["GET", "POST"],
            resources=[
                r"^/catalog/catalogs/[^/]+/(reprovision|dead-letter)(/.*)?$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    config={"required_roles": [IamRolesConfig().admin_role_name]},
                )
            ],
        ),
    ]


def catalog_status_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the catalog_status extension's role bindings.

    ``catalog_status_read`` is bound to the anonymous role so that public
    catalogs are accessible without authentication (same pattern as the STAC
    and OGC read surfaces).  When the platform restricts anonymous access, the
    anonymous role carries no read grants and the 404-gate remains the only
    protection.

    ``catalog_status_admin`` is intentionally left unbound here: operators
    wire catalog-admin authority through the DB-resident flow, exactly as
    ``admin_catalog_access`` is left unbound in the admin extension.  Sysadmin
    and admin reach the mutation routes via the condition-gate (they satisfy
    ``catalog_admin_required`` by default).
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        # Read surface: open to any authenticated or anonymous caller;
        # 404-on-hidden is the actual access control.
        Role(name="anonymous", policies=["catalog_status_read"]),
        # Mutation surface: sysadmin and admin satisfy catalog_admin_required.
        Role(name=sysadmin_role_name, policies=["catalog_status_admin"]),
        Role(name=admin_role_name, policies=["catalog_status_admin"]),
    ]
