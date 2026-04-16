"""
CI guard: no driver-class isinstance checks in service/extension code.

Invariant:
    grep -R 'isinstance.*Postgresql.*Driver' src/dynastore/extensions src/dynastore/modules/catalog
    returns zero hits.

Any new isinstance check on a concrete driver class in extensions or catalog
services is an architecture violation. Use Capability constants instead.
"""

import re
import subprocess
from pathlib import Path

# Directories where driver isinstance leakage is forbidden
_GUARDED_DIRS = [
    "src/dynastore/extensions",
    "src/dynastore/modules/catalog",
]

# Pattern: isinstance(something, <DriverName>)
_PATTERN = r"isinstance\([^,]+,\s*(?:Collection|Asset|Metadata)(?:Postgresql|Elasticsearch|ElasticsearchObfuscated|Iceberg|Duckdb)Driver\b"

_REPO_ROOT = Path(__file__).parents[6]  # geoid/


def _grep_for_violations() -> list[str]:
    """Return list of 'file:line: match' strings for each violation."""
    violations = []
    for rel_dir in _GUARDED_DIRS:
        target = _REPO_ROOT / rel_dir
        if not target.exists():
            continue
        for py_file in target.rglob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if re.search(_PATTERN, line):
                    violations.append(f"{py_file.relative_to(_REPO_ROOT)}:{lineno}: {line.strip()}")
    return violations


def test_no_driver_class_isinstance_in_services():
    """No isinstance check on a concrete driver class in extensions or catalog modules."""
    violations = _grep_for_violations()
    assert violations == [], (
        "Driver isinstance leak(s) found — use Capability constants instead:\n"
        + "\n".join(f"  {v}" for v in violations)
    )
