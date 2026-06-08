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

"""Regression test — the lifespan must trigger built-in registrations BEFORE
calling `seed_platform_notebooks`.

The original `dynastore.modules.notebooks.__init__` did the four built-in
re-imports at package load time, so by the time `NotebooksModule.lifespan()`
ran, the in-memory `_platform_registry` was already populated and
`seed_platform_notebooks` happily wrote rows into the platform_notebooks
table.

Phase A of PR #127 deferred those imports into `lifespan()` to break the
SCOPE-trimmed Cloud Run Job ImportError chain. The first cut placed the
imports AFTER `seed_platform_notebooks`, which silently regresses
behaviour: the seed reads an empty registry and writes nothing, so every
fresh deploy has no built-in platform notebooks.

This test parses the lifespan source and asserts the import block precedes
the seed call. Source-level introspection avoids needing a real DB engine.
"""
import inspect

from dynastore.modules.notebooks.notebooks_module import NotebooksModule


def test_lifespan_imports_run_before_seed_platform_notebooks() -> None:
    src = inspect.getsource(NotebooksModule.lifespan)

    seed_marker = "seed_platform_notebooks(conn)"
    import_marker = '__import__(mod_path)'

    seed_idx = src.find(seed_marker)
    import_idx = src.find(import_marker)

    assert seed_idx != -1, (
        "Could not find seed_platform_notebooks call — has the lifespan "
        "been refactored away from this pattern? Update the regression test."
    )
    assert import_idx != -1, (
        "Could not find the built-in __import__ block — has the lifespan "
        "been refactored away from this pattern? Update the regression test."
    )
    assert import_idx < seed_idx, (
        "REGRESSION: built-in notebook submodule imports MUST run BEFORE "
        "seed_platform_notebooks(conn). The seed reads the in-memory "
        "_platform_registry which is populated as a side effect of those "
        "imports — running it first writes an empty seed and every fresh "
        "deploy ends up with zero built-in platform notebooks."
    )
