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

"""Pure declarations of the Notebooks extension's authz policies.

Consumed by IAM via ``NotebooksExtension.get_policies`` /
``get_role_bindings`` through the structural ``PolicyContributor``
duck-type and forwarded by ``PolicyContributorPreset``
(see ``modules/storage/presets/policy_contributor_adapter.py``).
"""

from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


_NOTEBOOKS_PUBLIC_POLICY_ID = "notebooks_public_access"


def notebooks_policies() -> List[Policy]:
    return [
        Policy(
            id=_NOTEBOOKS_PUBLIC_POLICY_ID,
            description=(
                "Allows anonymous read access to platform notebooks and the "
                "notebooks web page."
            ),
            actions=["GET", "OPTIONS"],
            resources=[
                "/notebooks/platform",
                "/notebooks/platform/.*",
                "/web/pages/notebooks",
            ],
            effect="ALLOW",
        ),
    ]


def notebooks_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    name = anonymous_role_name or IamRolesConfig().anonymous_role_name
    return [Role(name=name, policies=[_NOTEBOOKS_PUBLIC_POLICY_ID])]
