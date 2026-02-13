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
from dynastore.models.auth import Policy, AuthorizationProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# Register public access policy for features extension
def register_features_policies():
    policy = Policy(
        id="features_public_access",
        description="Allows anonymous access to OGC API Features endpoints.",
        actions=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        resources=[
            "/features.*",
            "/features/.*"
        ]
    )
    
    authz = get_protocol(AuthorizationProtocol)
    if authz:
        authz.register_policy(policy)
        
        # Also attach to anonymous role so it's effective for unauthenticated users
        from dynastore.modules.apikey.models import Role
        from dynastore.modules.apikey.policies import register_role
        register_role(Role(
            name="anonymous",
            description="Anonymous user with limited access.",
            policies=["features_public_access"],
            is_system=True
        ))
        
        logger.debug("OGC Features policies registered and attached to anonymous role via AuthorizationProtocol.")
    else:
        logger.warning("No AuthorizationProtocol implementer found. OGC Features policies skipped.")
