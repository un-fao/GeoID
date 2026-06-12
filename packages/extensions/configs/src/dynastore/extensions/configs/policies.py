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

"""Pure declarations of the Configs extension's authz policies.

Consumed by IAM via ``ConfigsService.get_policies`` /
``get_role_bindings`` through the structural ``PolicyContributor``
duck-type and forwarded by ``PolicyContributorPreset``
(see ``modules/storage/presets/policy_contributor_adapter.py``).
"""

from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


_CONFIGS_ACCESS_POLICY_ID = "configs_access"


def configs_policies() -> List[Policy]:
    return [
        Policy(
            id=_CONFIGS_ACCESS_POLICY_ID,
            description="Grants full access to configuration management endpoints and "
                        "the Configuration Hub / Presets web pages.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/configs",
                "/configs/",
                "/configs/.*",
                "/web/pages/configuration",
                "/web/pages/presets",
                "/web/configs/.*",
            ],
            effect="ALLOW",
        ),
    ]


def configs_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Bind ``configs_access`` to ``sysadmin`` and ``admin`` roles by default.

    When IAM is absent, these bindings are never consulted and the
    ``audience_policy_id`` on the web pages is not enforced, so the pages
    are visible to all users (which is the intended IAM-absent behaviour).
    """
    from dynastore.models.protocols.authorization import IamRolesConfig
    cfg = IamRolesConfig()
    sysadmin = sysadmin_role_name or cfg.sysadmin_role_name
    admin = admin_role_name or cfg.admin_role_name
    return [
        Role(name=sysadmin, policies=[_CONFIGS_ACCESS_POLICY_ID]),
        Role(name=admin, policies=[_CONFIGS_ACCESS_POLICY_ID]),
    ]
