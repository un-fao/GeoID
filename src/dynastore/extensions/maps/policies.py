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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_maps_policies():
    """Register Maps public-access policy and anonymous role via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; maps policies not registered.")
        return

    maps_policy = Policy(
        id="maps_public_access",
        description="Allows anonymous GET access to Maps tile and viewer endpoints.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/maps",
            "/maps/",
            "/maps/.*",
            "/web/pages/map_viewer",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    pm.register_policy(maps_policy)
    pm.register_role(Role(name="anonymous", policies=["maps_public_access"]))

    logger.debug("Maps policies registered via PermissionProtocol.")
