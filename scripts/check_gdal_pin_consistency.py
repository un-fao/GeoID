#!/usr/bin/env python3
"""Guard against GDAL version-pin drift across the repo.

GDAL's version is pinned independently in five places that MUST stay in
lockstep. Drift between them caused a CI outage (#1040). This checker parses
the version from every location and fails loudly if any disagree.

Pin locations (keep in sync; bump all together):
  1-2. packages/core/src/dynastore/docker/Dockerfile
       two build-stage base images `FROM ghcr.io/osgeo/gdal:ubuntu-full-<X>`
  3.   pyproject.toml (root) `module_gdal` extra `GDAL==<X>`
  4.   pyproject.toml (root) `extension_maps` extra `GDAL==<X>`
  5.   packages/extensions/gdal/pyproject.toml dependency `GDAL==<X>`
  6.   packages/extensions/maps/pyproject.toml dependency `GDAL==<X>`

Pure stdlib, Python 3.12+. Exit 0 if all agree, exit 1 otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DOCKERFILE = REPO_ROOT / "packages/core/src/dynastore/docker/Dockerfile"
ROOT_PYPROJECT = REPO_ROOT / "pyproject.toml"
GDAL_EXT_PYPROJECT = REPO_ROOT / "packages/extensions/gdal/pyproject.toml"
MAPS_EXT_PYPROJECT = REPO_ROOT / "packages/extensions/maps/pyproject.toml"

# Image tag form: FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.4 AS builder
DOCKER_TAG_RE = re.compile(
    r"^\s*FROM\s+ghcr\.io/osgeo/gdal:ubuntu-full-([0-9][0-9A-Za-z.\-]*)\b"
)
# Pip pin form: "GDAL==3.12.4"
GDAL_PIN_RE = re.compile(r"""GDAL\s*==\s*([0-9][0-9A-Za-z.\-]*)""")


class Found:
    """A single discovered pin: where it lives and the version string."""

    def __init__(self, label: str, version: str) -> None:
        self.label = label
        self.version = version


def _read(path: Path) -> str:
    if not path.is_file():
        print(f"ERROR: expected file not found: {path}", file=sys.stderr)
        sys.exit(2)
    return path.read_text(encoding="utf-8")


def find_dockerfile_tags() -> list[Found]:
    """Return the version from each `FROM ...gdal:ubuntu-full-<X>` build stage."""
    found: list[Found] = []
    rel = DOCKERFILE.relative_to(REPO_ROOT)
    for lineno, line in enumerate(_read(DOCKERFILE).splitlines(), start=1):
        m = DOCKER_TAG_RE.match(line)
        if m:
            found.append(Found(f"{rel}:{lineno} (FROM ubuntu-full)", m.group(1)))
    return found


def find_pin(path: Path, marker: str | None = None) -> list[Found]:
    """Return every `GDAL==<X>` pin in *path*.

    When *marker* is given, only lines containing that substring are reported
    (used to disambiguate the two extras inside the root pyproject.toml).
    """
    found: list[Found] = []
    rel = path.relative_to(REPO_ROOT)
    for lineno, line in enumerate(_read(path).splitlines(), start=1):
        if marker is not None and marker not in line:
            continue
        m = GDAL_PIN_RE.search(line)
        if m:
            tag = f" ({marker})" if marker else ""
            found.append(Found(f"{rel}:{lineno}{tag}", m.group(1)))
    return found


def main() -> int:
    findings: list[Found] = []
    findings += find_dockerfile_tags()
    findings += find_pin(ROOT_PYPROJECT, marker="module_gdal")
    findings += find_pin(ROOT_PYPROJECT, marker="extension_maps")
    findings += find_pin(GDAL_EXT_PYPROJECT)
    findings += find_pin(MAPS_EXT_PYPROJECT)

    expected = 6  # 2 Dockerfile tags + 4 GDAL== pins
    print("GDAL pin locations found:")
    for f in findings:
        print(f"  {f.label} -> {f.version}")

    if len(findings) != expected:
        print(
            f"\nFAIL: expected {expected} GDAL pins but found {len(findings)}. "
            "A pin location may have moved or been removed — update "
            "scripts/check_gdal_pin_consistency.py to match.",
            file=sys.stderr,
        )
        return 1

    versions = {f.version for f in findings}
    if len(versions) == 1:
        print(f"\nOK: all {expected} GDAL pins agree on {versions.pop()}")
        return 0

    print("\nFAIL: GDAL version-pin drift detected.", file=sys.stderr)
    print(f"  versions seen: {', '.join(sorted(versions))}", file=sys.stderr)
    for f in findings:
        print(f"  {f.label} -> {f.version}", file=sys.stderr)
    print(
        "\nBump all pins to the same version (see header of this script "
        "and Dockerfile for the full list).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
