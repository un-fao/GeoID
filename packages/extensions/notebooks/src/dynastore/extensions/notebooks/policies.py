#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

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
