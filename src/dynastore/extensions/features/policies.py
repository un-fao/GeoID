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


def register_features_policies():
    """Register OGC Features public-access policy and anonymous role.

    Always registers into the module-level in-memory registry so that
    provision_default_policies() picks it up at lifespan startup,
    regardless of whether PermissionProtocol is already available.
    """
    from dynastore.modules.apikey.policies import (
        register_policy as _reg_policy,
        register_role as _reg_role,
    )
    from dynastore.models.protocols.policies import PermissionProtocol

    features_policy = Policy(
        id="features_public_access",
        description="Allows anonymous access to OGC API Features endpoints.",
        actions=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        resources=[
            "/features.*",
            "/features/.*",
        ],
        effect="ALLOW",
    )
    _reg_policy(features_policy)
    _reg_role(Role(
        name="anonymous",
        description="Anonymous user with limited access.",
        policies=["features_public_access"],
        is_system=True,
    ))

    logger.debug("OGC Features policies pre-registered into in-memory registry.")

    # If PermissionProtocol is already live, push immediately.
    policy_manager = get_protocol(PermissionProtocol)
    if policy_manager:
        policy_manager.register_policy(features_policy)
        policy_manager.register_role(Role(
            name="anonymous",
            description="Anonymous user with limited access.",
            policies=["features_public_access"],
            is_system=True,
        ))
        logger.debug("OGC Features policies also applied to live PermissionProtocol.")
