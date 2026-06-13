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

"""Shared OGC declaration helpers — pure data, no side effects.

OGC service extensions (Coverages, Features, Records, EDR, DGGS, Joins,
Volumes, etc.) all share the same anonymous-public-access policy
shape: ``GET/OPTIONS`` on ``/{prefix}/...``. These helpers produce the
``Policy`` / ``Role`` model declarations; the IAM module's
``PolicyContributor`` consumer reads them via each service's
``get_policies`` / ``get_role_bindings`` methods and forwards to
``PermissionProtocol`` centrally.

Callers never touch ``PermissionProtocol`` themselves — see PR #308
for the architectural rule.
"""

import logging
from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig

logger = logging.getLogger(__name__)


def ogc_public_access_policy(
    protocol_prefix: str,
    extra_resources: Optional[List[str]] = None,
) -> Policy:
    """Pure declaration of the ``{prefix}_public_access`` policy.

    Args:
        protocol_prefix: The URL prefix without leading slash
                         (e.g. ``"features"``, ``"stac"``, ``"coverages"``).
        extra_resources: Additional resource patterns to include
                         (e.g. ``["/web/pages/stac_browser"]``).
    """
    resources = [
        f"/{protocol_prefix}",
        f"/{protocol_prefix}/",
        f"/{protocol_prefix}/.*",
    ]
    if extra_resources:
        resources.extend(extra_resources)
    return Policy(
        id=f"{protocol_prefix}_public_access",
        description=f"Allows anonymous access to OGC API {protocol_prefix.title()} endpoints.",
        actions=["GET", "OPTIONS"],
        resources=resources,
        effect="ALLOW",
    )


def ogc_anonymous_role_binding(
    protocol_prefix: str,
    anonymous_role_name: Optional[str] = None,
) -> Role:
    """Pure declaration of the anonymous-role binding for ``{prefix}_public_access``.

    Args:
        protocol_prefix: Same prefix passed to ``ogc_public_access_policy``.
        anonymous_role_name: Role name to bind. Defaults to
                            ``IamRolesConfig().anonymous_role_name``;
                            operators wiring a custom role landscape pass
                            an explicit name.
    """
    return Role(
        name=anonymous_role_name or IamRolesConfig().anonymous_role_name,
        policies=[f"{protocol_prefix}_public_access"],
    )
