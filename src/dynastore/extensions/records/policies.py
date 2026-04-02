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

"""ABAC policy registration for OGC API - Records endpoints."""

import logging

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_records_policies() -> None:
    """Register OGC Records public-access policy via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning(
            "PermissionProtocol not available; records policies not registered."
        )
        return

    from dynastore.models.auth_models import Policy, Role

    records_policy = Policy(
        id="records_public_access",
        description="Allows anonymous access to OGC API Records endpoints.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/records.*",
            "/records/.*",
        ],
        effect="ALLOW",
    )
    pm.register_policy(records_policy)
    pm.register_role(
        Role(
            name="anonymous",
            description="Anonymous user with limited access.",
            policies=["records_public_access"],
        )
    )

    logger.debug("OGC Records policies registered via PermissionProtocol.")
