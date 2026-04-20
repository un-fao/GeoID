#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Boundary guard — generic services must never import driver-specific classes.

The role-based driver refactor (plan §Principle — load-bearing invariant)
says the generic tier adapters — ``CollectionService``, ``CatalogService``,
``AssetService`` — contain zero driver-specific imports / isinstance checks /
construction.  Driver-specific typing, persistence, and defaults live inside
driver-registered ``lifecycle_registry`` hooks (e.g. the PG
``init_collection`` hook in ``modules/storage/drivers/postgresql.py``).

This test enforces the invariant with a plain-text grep.  It's deliberately
simple — we want the rule obvious and the failure mode obvious.  Adding a
driver-specific import to any of the guarded services fails this test.
"""

from pathlib import Path

import pytest

_REPO_SRC = Path(__file__).resolve().parents[5] / "src" / "dynastore"

_GUARDED_SERVICES = [
    _REPO_SRC / "modules" / "catalog" / "collection_service.py",
    _REPO_SRC / "modules" / "catalog" / "catalog_service.py",
    _REPO_SRC / "modules" / "catalog" / "asset_service.py",
]

# Symbols that generic services must NOT import or reference at the
# expression level (comments and docstrings are exempt — a line is ignored
# if the forbidden token only appears after a `#` character).
_FORBIDDEN_TOKENS = [
    "SidecarRegistry",
    "CollectionPostgresqlDriverConfig",
    "AssetPostgresqlDriverConfig",
]


def _strip_comment(line: str) -> str:
    """Return the code portion of a line (everything before the first `#`).

    Crude — doesn't handle `#` inside string literals — but good enough for
    detecting forbidden identifier uses in real code, which never puts
    the symbol name inside a `#`-quoted string.
    """
    idx = line.find("#")
    return line if idx == -1 else line[:idx]


@pytest.mark.parametrize(
    "service_path",
    _GUARDED_SERVICES,
    ids=lambda p: p.name,
)
def test_service_has_no_driver_imports(service_path: Path) -> None:
    assert service_path.exists(), f"Guarded service file missing: {service_path}"
    src = service_path.read_text()

    violations: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(src.splitlines(), start=1):
        code = _strip_comment(line)
        for token in _FORBIDDEN_TOKENS:
            if token in code:
                violations.append((lineno, token, line.rstrip()))

    if violations:
        report = "\n".join(
            f"  {service_path.name}:{lineno}: {token}  →  {line}"
            for lineno, token, line in violations
        )
        pytest.fail(
            f"Generic/driver boundary broken in {service_path.name}. "
            f"Driver-specific symbols must stay inside driver-registered "
            f"lifecycle hooks, not in generic services:\n{report}"
        )
