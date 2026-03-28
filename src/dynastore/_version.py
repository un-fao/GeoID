"""Single source of truth for DynaStore version at runtime."""

import importlib.metadata
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_PACKAGE_NAME = "DynaStore"
_VERSION_UNKNOWN = "0.0.0-unknown"


def get_version() -> str:
    """Return the package version using a three-tier fallback."""
    # Tier 1: installed package metadata (pip install, Docker runtime)
    try:
        return importlib.metadata.version(_PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    # Tier 2: VERSION file at repository root (editable install, PYTHONPATH dev)
    for candidate in (
        Path(__file__).resolve().parents[3] / "VERSION",  # src/dynastore/_version.py -> repo root
        Path(__file__).resolve().parents[2] / "VERSION",  # fallback
    ):
        if candidate.is_file():
            try:
                return candidate.read_text().strip()
            except OSError:
                pass
    logger.warning("Could not determine DynaStore version; using fallback.")
    return _VERSION_UNKNOWN


def get_git_commit() -> str:
    """Return the short git commit hash, or 'unknown' in non-git environments."""
    # Prefer build-time injection (Docker images have no .git)
    build_commit = os.environ.get("BUILD_COMMIT", "").strip()
    if build_commit and build_commit != "unknown":
        return build_commit
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).decode().strip()
    except Exception:
        return "unknown"


def get_build_info() -> dict:
    """Return version info safe for public exposure."""
    return {
        "version": get_version(),
        "commit": get_git_commit(),
        "build_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


VERSION = get_version()
