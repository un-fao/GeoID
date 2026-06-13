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

"""Regression test for geoid#2042: IdpConfig must be registered in
TypedModelRegistry at module-import time, not deferred inside a lifespan.

The config_seeder (TasksModule priority=15) calls
``resolve_config_class("idp_config")`` before IamModule lifespan fires
(priority=100).  On the old code, IdpConfig was only imported inside
``_register_identity_provider()``, so the registry lookup returned None and
the idp-config.json overlay was silently skipped on every fresh boot.

The fix adds a top-level import of IdpConfig in module.py so the
PluginConfig.__init_subclass__ registration runs as soon as the IAM module
entry point is loaded — before any lifespan is invoked.

This test verifies the fix by:
1. Temporarily evicting the IAM module from sys.modules so the import runs
   fresh (as if this were the first boot).
2. Mocking the scope-gate distribution check to pass (the test env does not
   install dynastore-ext-iam, but a real deployment does).
3. Importing the entry-point module without directly naming IdpConfig.
4. Asserting resolve_config_class("idp_config") returns the expected class.

Reverting the top-level ``from .idp_config import IdpConfig`` in module.py
would make step 4 return None, failing this test.
"""
from __future__ import annotations

import importlib
import importlib.metadata
import os
import sys
from unittest.mock import patch

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)


def _drop_iam_module_from_cache() -> None:
    """Evict the IAM module entry-point from sys.modules so the next import
    re-executes the module body (including all top-level imports).

    Only the module.py file is evicted; idp_config.py may already be in
    sys.modules from other tests — that is fine because the real invariant is
    that importing module.py is *sufficient* to trigger the registration, not
    that idp_config was previously unknown.
    """
    for name in list(sys.modules):
        if name == "dynastore.modules.iam.module" or name.startswith(
            "dynastore.modules.iam.module."
        ):
            sys.modules.pop(name, None)


def test_idp_config_registered_by_module_import() -> None:
    """Importing the IAM module entry point registers IdpConfig without any
    prior direct import of that class.

    The module has a distribution-presence scope gate that raises ImportError
    when dynastore-ext-iam is absent.  We patch that check to pass so we can
    test the import-time side-effects without needing the full IAM extras
    installed — mirroring what happens in a real deployment where the
    distribution IS present.
    """
    _drop_iam_module_from_cache()

    real_distribution = importlib.metadata.distribution

    def _allow_iam_dist(name: str):
        """Let the scope gate succeed; leave all other dist lookups real."""
        if name == "dynastore-ext-iam":
            # Return any valid distribution object; the gate only checks
            # that the call does NOT raise PackageNotFoundError.
            return real_distribution("dynastore")
        return real_distribution(name)

    with patch.object(importlib.metadata, "distribution", _allow_iam_dist):
        # Import ONLY the entry-point module — do not name IdpConfig directly.
        # This mirrors what plugin discovery does at startup.
        importlib.import_module("dynastore.modules.iam.module")

    # Now verify the registration happened as a side-effect of the import.
    from dynastore.models.plugin_config import resolve_config_class

    cls = resolve_config_class("idp_config")

    assert cls is not None, (
        "resolve_config_class('idp_config') returned None after importing "
        "dynastore.modules.iam.module. IdpConfig is not registered at "
        "module-import time — the config_seeder will skip idp-config.json "
        "overlays on first boot (geoid#2042 regression)."
    )
    assert cls.class_key() == "idp_config", (
        f"Expected class_key 'idp_config', got {cls.class_key()!r}"
    )

    # Clean up so the evicted module does not leak into later tests.
    _drop_iam_module_from_cache()
