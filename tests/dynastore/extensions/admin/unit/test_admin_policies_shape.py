#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Lock in the admin-extension policy + role-binding shape (#723).

These bindings are load-bearing: a stray rename or unbinding silently
shifts the catalog-admin surface back to the all-or-nothing
``admin_access`` model.
"""
from __future__ import annotations

from dynastore.extensions.admin.policies import (
    admin_policies,
    admin_role_bindings,
)


def test_admin_policies_ids_pinned():
    ids = [p.id for p in admin_policies()]
    assert ids == [
        "admin_access",
        "admin_catalog_access",
        "admin_catalogs_list",
        "admin_principal_lookup",
    ]


def test_admin_principal_lookup_targets_principals_route_only():
    pol = next(p for p in admin_policies() if p.id == "admin_principal_lookup")
    assert pol.actions == ["GET"]
    assert pol.resources == [r"^/admin/principals/?$"]
    assert pol.effect == "ALLOW"


def test_catalog_admin_sentinel_binds_both_lookup_policies():
    bindings = {r.name: list(r.policies) for r in admin_role_bindings()}
    assert "catalog_admin" in bindings
    assert set(bindings["catalog_admin"]) == {
        "admin_catalogs_list",
        "admin_principal_lookup",
    }


def test_sysadmin_and_admin_unchanged():
    """Privileged platform roles must NOT pick up the narrow lookup
    policies — they reach those routes via admin_access, and an
    accidental binding would let an operator break admin_access without
    noticing a privilege regression.
    """
    bindings = {r.name: list(r.policies) for r in admin_role_bindings()}
    assert bindings.get("sysadmin") == ["admin_access"]
    assert bindings.get("admin") == ["admin_access"]
