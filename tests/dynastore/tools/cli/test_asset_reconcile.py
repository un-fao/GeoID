"""Smoke tests for `python -m dynastore.tools.cli.asset_reconcile`."""

import subprocess
import sys


def test_help_runs_and_exits_zero():
    result = subprocess.run(
        [sys.executable, "-m", "dynastore.tools.cli.asset_reconcile", "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert "asset reconcile" in result.stdout.lower() or "drift" in result.stdout.lower()
    assert "--catalog" in result.stdout
    assert "--apply" in result.stdout


def test_collection_without_catalog_exits_2():
    result = subprocess.run(
        [
            sys.executable, "-m", "dynastore.tools.cli.asset_reconcile",
            "--collection", "c1",
        ],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 2
    assert "--collection requires --catalog" in result.stderr
