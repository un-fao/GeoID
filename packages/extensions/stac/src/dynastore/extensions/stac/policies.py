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

"""Pure declarations of the STAC extension's authz policies.

Consumed by IAM via ``STACService.get_policies`` / ``get_role_bindings``
through the ``PolicyContributor`` Protocol — see PR #308.

Implementation delegates to the shared OGC helpers in
``extensions/tools/ogc_policies.py`` since the STAC shape (anonymous
GET on ``/stac/...`` plus the ``stac_browser`` page) is the standard
OGC public-access pattern with one extra resource.
"""

from typing import List, Optional

from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


def stac_policies() -> List[Policy]:
    return [
        ogc_public_access_policy(
            "stac",
            extra_resources=["/web/pages/stac_browser"],
        ),
    ]


def stac_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [ogc_anonymous_role_binding("stac", anonymous_role_name)]
