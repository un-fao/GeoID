#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

"""Pure declarations of the Configs extension's authz policies.

Consumed by IAM via ``ConfigsService.get_policies`` /
``get_role_bindings`` through the structural ``PolicyContributor``
duck-type and forwarded by ``PolicyContributorPreset``
(see ``modules/storage/presets/policy_contributor_adapter.py``).
"""

from typing import List

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


_CONFIGS_ACCESS_POLICY_ID = "configs_access"


def configs_policies() -> List[Policy]:
    return [
        Policy(
            id=_CONFIGS_ACCESS_POLICY_ID,
            description="Grants full access to configuration management endpoints.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/configs",
                "/configs/",
                "/configs/.*",
                "/web/pages/configs_editor",
            ],
            effect="ALLOW",
        ),
    ]


def configs_role_bindings() -> List[Role]:
    # No role binding: ``configs_access`` is reachable via ``sysadmin_full_access``
    # already; operator can bind it to additional roles via the IAM admin UI.
    return []
