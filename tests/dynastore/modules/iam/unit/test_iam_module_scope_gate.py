"""Regression test for the IamModule SCOPE gate (#1003).

Background: ``IamModule`` lives in ``dynastore-core`` (always installed) but
must only register itself when the SCOPE includes the ``iam`` extras.  The
old import-side-effect gate (``from tenacity import …``) broke when tenacity
became transitively available via ``drivers_grp → pyiceberg``, and the
follow-up ``import jwt`` gate has the same hazard (PyJWT is reachable via
``module_gcp → gcloud-aio-storage → gcloud-aio-auth``).

The current gate checks for the ``dynastore-ext-iam`` distribution — a name
that cannot be installed by any transitive path.  This test pins that
mechanism so a future refactor does not regress to a package-import gate.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
from unittest.mock import patch


def _drop_iam_module_from_cache():
    for name in list(sys.modules):
        if name == "dynastore.modules.iam.module" or name.startswith(
            "dynastore.modules.iam.module."
        ):
            sys.modules.pop(name, None)


def test_iam_module_loads_when_distribution_present():
    """In a fully-installed dev env the distribution exists and import succeeds."""
    _drop_iam_module_from_cache()
    mod = importlib.import_module("dynastore.modules.iam.module")
    assert hasattr(mod, "IamModule")


def test_iam_module_raises_import_error_when_distribution_missing():
    """Gate fires: ``discover_and_load_plugins`` will skip IamModule on
    SCOPEs that exclude the ``iam`` extras (``dynastore-ext-iam`` absent)."""
    _drop_iam_module_from_cache()

    real_distribution = importlib.metadata.distribution

    def fake_distribution(name: str):
        if name == "dynastore-ext-iam":
            raise importlib.metadata.PackageNotFoundError(name)
        return real_distribution(name)

    with patch.object(importlib.metadata, "distribution", fake_distribution):
        import pytest
        with pytest.raises(ImportError, match="dynastore-ext-iam"):
            importlib.import_module("dynastore.modules.iam.module")

    _drop_iam_module_from_cache()
