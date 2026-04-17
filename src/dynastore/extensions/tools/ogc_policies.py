#    Copyright 2025 FAO
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

"""Shared OGC policy registration helper."""

import logging
from typing import List, Optional

from dynastore.models.protocols.authorization import DefaultRole
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_ogc_public_access_policy(
    protocol_prefix: str,
    extra_resources: Optional[List[str]] = None,
) -> None:
    """Register a standard OGC public-access policy and anonymous role.

    Creates a policy allowing anonymous GET/OPTIONS access to all
    endpoints under ``/{protocol_prefix}/...`` and registers it
    on the ``anonymous`` role via ``PermissionProtocol``.

    Args:
        protocol_prefix: The URL prefix without leading slash
                         (e.g. ``"features"``, ``"stac"``, ``"coverages"``).
        extra_resources: Additional resource patterns to include
                         (e.g. ``["/web/pages/stac_browser"]``).
    """
    from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning(
            "PermissionProtocol not available; %s policies not registered.",
            protocol_prefix,
        )
        return

    resources = [
        f"/{protocol_prefix}",
        f"/{protocol_prefix}/",
        f"/{protocol_prefix}/.*",
    ]
    if extra_resources:
        resources.extend(extra_resources)

    policy = Policy(
        id=f"{protocol_prefix}_public_access",
        description=f"Allows anonymous access to OGC API {protocol_prefix.title()} endpoints.",
        actions=["GET", "OPTIONS"],
        resources=resources,
        effect="ALLOW",
    )
    pm.register_policy(policy)
    pm.register_role(Role(name=DefaultRole.ANONYMOUS.value, policies=[f"{protocol_prefix}_public_access"]))

    logger.debug("OGC %s policies registered via PermissionProtocol.", protocol_prefix)
