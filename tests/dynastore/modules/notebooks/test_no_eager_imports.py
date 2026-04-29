"""Regression test for B2: notebooks package init must not pull module-specific deps.

The notebooks package historically eagerly re-imported four submodules
(`modules.catalog.notebooks`, `modules.storage.notebooks`,
`modules.elasticsearch.notebooks`, `tasks.ingestion.notebooks`) at the bottom
of `__init__.py`. Each of those transitively pulls heavy deps (e.g. shapely
via storage's pg_sidecars/geometries, elasticsearch via the ES module).

When a SCOPE-trimmed Cloud Run Job image (e.g.
`dynastore-dimensions-materialize-job`) imports the notebooks package, those
deps would crash the container. Registration now happens lazily inside
`NotebooksModule.lifespan()`.

These tests poison sys.modules to simulate a missing dep and assert that
importing the package and the public re-exports still works.
"""
import importlib
import sys

import pytest


_LIGHTWEIGHT_PACKAGE_IMPORTS = (
    "dynastore.modules.notebooks",
    "dynastore.modules.notebooks.example_registry",
    "dynastore.modules.notebooks.models",
    "dynastore.modules.notebooks.notebooks_module",
    "dynastore.modules.notebooks.notebooks_db",
    "dynastore.modules.notebooks.platform_db",
)

_SHOULD_NOT_BE_PULLED = (
    "shapely",
    "shapely.geometry",
    "elasticsearch",
)


def _purge_notebooks_modules():
    """Drop cached notebooks-related modules so the next import re-runs the
    package init under the poisoned sys.modules."""
    for name in list(sys.modules):
        if name == "dynastore.modules.notebooks" or name.startswith(
            "dynastore.modules.notebooks."
        ):
            del sys.modules[name]


@pytest.mark.parametrize("missing_dep", _SHOULD_NOT_BE_PULLED)
def test_notebooks_package_imports_without_module_deps(missing_dep: str) -> None:
    """Pre-poison a heavy module dep, then import the notebooks package.

    Should not raise — the four module-specific notebook submodules must NOT
    be pulled at package-init time. They are loaded lazily by
    NotebooksModule.lifespan().
    """
    _purge_notebooks_modules()
    original = sys.modules.get(missing_dep)
    sys.modules[missing_dep] = None  # type: ignore[assignment]
    try:
        for mod_path in _LIGHTWEIGHT_PACKAGE_IMPORTS:
            importlib.import_module(mod_path)
    finally:
        if original is not None:
            sys.modules[missing_dep] = original
        else:
            sys.modules.pop(missing_dep, None)


def test_register_platform_notebook_is_callable_after_import() -> None:
    """`register_platform_notebook` from the package init re-export must be the
    same function as the one in `example_registry`."""
    from dynastore.modules.notebooks import register_platform_notebook as pkg_fn
    from dynastore.modules.notebooks.example_registry import (
        register_platform_notebook as deep_fn,
    )

    assert pkg_fn is deep_fn
