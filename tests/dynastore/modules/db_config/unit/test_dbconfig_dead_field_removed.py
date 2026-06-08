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

"""Regression test: ``DBConfig.pool_max_inactive_connection_lifetime`` was dead.

Background: the field was declared at ``db_config/db_config.py:29`` as::

    pool_max_inactive_connection_lifetime: int = int(
        os.getenv("DB_POOL_MAX_INACTIVE_CONNECTION_LIFETIME", "30")
    )

with **zero** consumers in the codebase (the async DB engine is SQLAlchemy
+ asyncpg, not a raw ``asyncpg.Pool`` — the lifetime knob it would have
fed lives on the asyncpg Pool object the engine doesn't expose).

The deployment configs in the sister ``dynastore`` and ``fao-aip-catalog``
repos still set ``DB_POOL_MAX_INACTIVE_CONNECTION_LIFETIME=300`` per env
expecting it to limit pool lifetime — silent no-op. ``#756`` cites this
field as the canonical precedent of the "Dead config" failure mode.

Fix: delete the field. Cleanup of the env var in the sister repos
(removed in companion ``dynastore`` + ``fao-aip-catalog`` PRs) is tracked
in the PR description.

This test pins absence of the field so a future paste of the line back
into ``DBConfig`` (or any field named ``pool_max_inactive*``) fails
loudly at unit-test time.
"""
from __future__ import annotations


def test_pool_max_inactive_connection_lifetime_not_on_dbconfig():
    """The field used to be a silent no-op operators tuned in vain;
    pin its absence so the rule against dead config (``#756``) doesn't
    regress."""
    from dynastore.modules.db_config.db_config import DBConfig

    for attr in vars(DBConfig):
        assert not attr.startswith("pool_max_inactive"), (
            f"DBConfig.{attr} was deleted as dead-config — the async "
            f"engine is SQLAlchemy, not a raw asyncpg.Pool, so the "
            f"lifetime knob has no consumer. Setting it via env var "
            f"was a silent no-op that misled operators."
        )


def test_no_consumer_grep_for_dead_field():
    """Companion source-level check: even outside DBConfig, no module
    should be reading ``pool_max_inactive_connection_lifetime``. If a
    consumer appears later, the deletion was premature and must be
    revisited together with the dynastore/fao-aip-catalog env-var
    cleanup in the companion PRs."""
    import pathlib

    # Walk this checkout's packages tree; the test file lives at
    # tests/dynastore/modules/db_config/unit/, so the repo root is 4
    # levels up.
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    packages = repo_root / "packages"
    assert packages.is_dir(), f"expected packages/ at {packages}"

    hits = []
    for py in packages.rglob("*.py"):
        try:
            text = py.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for needle in (
            "pool_max_inactive_connection_lifetime",
            "DB_POOL_MAX_INACTIVE_CONNECTION_LIFETIME",
        ):
            if needle in text:
                # Allow the needle to appear ONLY inside this test file
                # (the assertion message itself contains it).
                if py == here:
                    continue
                hits.append(f"{py.relative_to(repo_root)} contains {needle!r}")

    assert hits == [], (
        "expected zero references to the deleted dead config field, "
        f"found: {hits!r}. If a consumer reappeared, restore the field "
        "AND the env-var exports in dynastore/fao-aip-catalog deployment "
        "configs (deleted together in the companion PRs)."
    )
