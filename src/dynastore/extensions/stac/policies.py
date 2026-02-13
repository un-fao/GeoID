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

# Register public access policy for stac extension
def register_stac_policies():
    policy = Policy(
        id="stac_public_access",
        description="Allows anonymous access to STAC endpoints.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/stac.*",
            "/stac/.*"
        ]
    )
    
    authz = get_protocol(AuthorizationProtocol)
    if authz:
        authz.register_policy(policy)
        logger.debug("STAC policies registered via AuthorizationProtocol.")
    else:
        logger.warning("No AuthorizationProtocol implementer found. STAC policies skipped.")
