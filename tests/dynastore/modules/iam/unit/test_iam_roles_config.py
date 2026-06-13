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

"""Unit tests for the ``IamRolesConfig`` PluginConfig (geoid#643 slice 2).

Pins the tier-split shape that replaces the single-list ``roles`` field:

- canonical roles are owned by the ``default_roles_baseline`` preset
  (platform: ``sysadmin``; catalog: ``admin, unauthenticated`` — ``editor``
  and ``user`` were dropped to minimize the always-on role surface);
- ``IamRolesConfig.platform_roles`` / ``catalog_roles`` hold OPERATOR-DEFINED
  extras on top of the canonical preset roles (default ``[]``);
- ``role_names`` / ``platform_role_names`` properties union canonical + extras;
- role inheritance is encoded on ``RoleSeed.parent``, not a separate
  ``hierarchy`` list — the trimmed default catalog tier has no edges
  (admin and unauthenticated are both top-level);
- named slots resolve to the correct tier — ``sysadmin_role_name`` must
  be in the platform union; ``admin/editor/default_user/anonymous`` slots
  must be in the catalog union; ``editor_role_name`` and
  ``default_user_role_name`` defaults collapse onto ``admin`` and
  ``unauthenticated`` respectively, since their historical targets are
  no longer provisioned;
- ``admin_tier_role_names`` may span tiers (default: ``[sysadmin, admin]``);
- cycles in the parent links are rejected;
- cross-tier parents are rejected (a catalog role cannot inherit from a
  platform role and vice-versa);
- role names are globally unique across tiers.
"""

import pytest

from dynastore.models.protocols.authorization import (
    IamRolesConfig,
    RoleSeed,
)
from dynastore.modules.iam.presets.default_roles_baseline import (
    DEFAULT_CATALOG_ROLES,
    DEFAULT_PLATFORM_ROLES,
)


def test_canonical_platform_seeds_contain_sysadmin_only() -> None:
    assert [r.name for r in DEFAULT_PLATFORM_ROLES] == ["sysadmin"]


def test_canonical_catalog_seeds_contain_two_roles() -> None:
    assert [r.name for r in DEFAULT_CATALOG_ROLES] == [
        "admin",
        "unauthenticated",
    ]
    # `viewer` was dropped (geoid#643 slice 1). `anonymous` was renamed
    # to `unauthenticated` (slice 2). `editor` and `user` were dropped to
    # minimize the always-on role surface — operators add them via PATCH
    # if their UI/workflows need that distinction.
    for forbidden in ("viewer", "anonymous", "editor", "user"):
        assert forbidden not in [r.name for r in DEFAULT_CATALOG_ROLES]


def test_canonical_catalog_chain_no_default_edges() -> None:
    """Catalog tier has no default inheritance edges — admin and
    unauthenticated are both top-level. Operators can add edges via
    ``IamProtocol.add_role_hierarchy``."""
    parents = {r.name: r.parent for r in DEFAULT_CATALOG_ROLES}
    assert parents == {
        "admin": None,
        "unauthenticated": None,
    }


def test_canonical_platform_sysadmin_has_no_parent() -> None:
    assert DEFAULT_PLATFORM_ROLES[0].parent is None


def test_config_defaults_to_empty_operator_extras() -> None:
    """Config-level fields default to ``[]`` — canonical roles live in the
    preset, not in the runtime-editable config."""
    cfg = IamRolesConfig()
    assert cfg.platform_roles == []
    assert cfg.catalog_roles == []


def test_default_named_slots() -> None:
    cfg = IamRolesConfig()
    assert cfg.sysadmin_role_name == "sysadmin"
    assert cfg.admin_role_name == "admin"
    # ``editor_role_name`` default collapses onto ``admin`` since the
    # ``editor`` catalog role is no longer provisioned by the baseline.
    assert cfg.editor_role_name == "admin"
    # ``default_user_role_name`` default collapses onto ``unauthenticated``
    # since the ``user`` catalog role is no longer provisioned.
    assert cfg.default_user_role_name == "unauthenticated"
    # Renamed default per slice 2.
    assert cfg.anonymous_role_name == "unauthenticated"


def test_admin_role_set_spans_tiers() -> None:
    """``admin_tier_role_names`` default spans tiers by design:
    sysadmin (platform) + admin (catalog)."""
    cfg = IamRolesConfig()
    assert cfg.admin_role_set == frozenset({"sysadmin", "admin"})


def test_role_names_property_unions_both_tiers() -> None:
    cfg = IamRolesConfig()
    assert set(cfg.role_names) == {
        "sysadmin",
        "admin",
        "unauthenticated",
    }


def test_platform_role_names_property_returns_platform_tier_only() -> None:
    cfg = IamRolesConfig()
    assert cfg.platform_role_names == frozenset({"sysadmin"})


def test_operator_added_catalog_role_flows_through() -> None:
    """An operator can append an extra catalog role via PATCH — verify it
    shows up in role_names alongside canonical roles and is excluded from
    the platform set."""
    cfg = IamRolesConfig(
        catalog_roles=[
            RoleSeed(name="reviewer", description="External reviewer",
                     policies=[], level=40, parent="admin"),
        ],
    )
    assert "reviewer" in cfg.role_names
    assert "reviewer" not in cfg.platform_role_names


def test_sysadmin_slot_must_be_in_platform_roles() -> None:
    with pytest.raises(ValueError, match="sysadmin_role_name"):
        IamRolesConfig(sysadmin_role_name="admin")  # admin is catalog-tier


def test_admin_slot_must_be_in_catalog_roles() -> None:
    with pytest.raises(ValueError, match="admin_role_name"):
        IamRolesConfig(admin_role_name="sysadmin")  # sysadmin is platform-tier


def test_anonymous_slot_must_be_in_catalog_roles() -> None:
    with pytest.raises(ValueError, match="anonymous_role_name"):
        IamRolesConfig(anonymous_role_name="sysadmin")


def test_admin_tier_role_names_must_be_known() -> None:
    with pytest.raises(ValueError, match="admin_tier_role_names"):
        IamRolesConfig(admin_tier_role_names=["sysadmin", "admin", "ghost"])


def test_parent_cycle_rejected() -> None:
    """A catalog-tier role cannot have a parent chain that loops."""
    with pytest.raises(ValueError, match="cycle"):
        IamRolesConfig(
            catalog_roles=[
                RoleSeed(name="admin", level=100, parent="editor"),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_cross_tier_parent_rejected() -> None:
    """A catalog role cannot point at a platform-tier role via `parent`."""
    with pytest.raises(ValueError, match="parent=|not in"):
        IamRolesConfig(
            catalog_roles=[
                RoleSeed(name="admin", level=100, parent="sysadmin"),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_role_name_uniqueness_across_tiers() -> None:
    """A name appearing in both tiers is rejected — names must be unique
    globally so grant-resolution is unambiguous."""
    with pytest.raises(ValueError, match="appear in BOTH"):
        IamRolesConfig(
            platform_roles=[
                RoleSeed(name="sysadmin", level=100),
                RoleSeed(name="admin", level=90),  # also in catalog tier
            ],
            catalog_roles=[
                RoleSeed(name="admin", level=100),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_config_address_is_platform_iam_roles() -> None:
    assert IamRolesConfig._address == ("platform", "modules", "iam", "roles")
