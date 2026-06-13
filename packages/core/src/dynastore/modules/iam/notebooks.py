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
