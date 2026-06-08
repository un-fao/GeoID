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
