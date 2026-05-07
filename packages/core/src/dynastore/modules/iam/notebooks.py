"""Platform notebook registrations for the IAM module."""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.iam"


register_platform_notebook(
    notebook_id="iam_provision_tenant_and_assign_roles",
    registered_by=_REG,
    notebook_path=_HERE / "iam01_provision_tenant_and_assign_roles.ipynb",
    title={"en": "IAM — Provision Tenant + Assign Roles"},
    tags=["iam", "tenant", "roles", "provisioning"],
)
register_platform_notebook(
    notebook_id="iam_self_service_permission_introspection",
    registered_by=_REG,
    notebook_path=_HERE / "iam02_self_service_permission_introspection.ipynb",
    title={"en": "IAM — Self-Service Permission Introspection"},
    tags=["iam", "permissions", "introspection"],
)
register_platform_notebook(
    notebook_id="iam_custom_roles_and_policies",
    registered_by=_REG,
    notebook_path=_HERE / "iam03_custom_roles_and_policies.ipynb",
    title={"en": "IAM — Custom Roles and Policies"},
    tags=["iam", "policies", "roles", "custom"],
)
