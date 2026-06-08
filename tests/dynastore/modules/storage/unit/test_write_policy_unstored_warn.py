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

"""#330 B5 — warn when a statistic derivation is computed but stored nowhere.

A ``geometry_stats`` / ``attribute_stats`` entry with ``store=None`` is computed
on every write but never persisted. That is legitimate only when an identity
rule references it (a compute-only match axis). When nothing references it the
value feeds nothing — the GLOSIS foot-gun — so the ItemsWritePolicy validate
handler logs a WARNING at config save.
"""

import asyncio
import logging

from dynastore.modules.storage.computed_fields import (
    ComputedKind,
    DeriveSpec,
    GeometryStat,
    IdentityRule,
    StatisticStorageMode,
)
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    _warn_unstored_unreferenced_stats,
)

_LOGGER_NAME = "dynastore.modules.storage.driver_config"


def _run(wp: ItemsWritePolicy, caplog) -> str:
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        asyncio.run(_warn_unstored_unreferenced_stats(wp, "cat", "col", None))
    return caplog.text


def test_warns_when_stat_unstored_and_unreferenced(caplog) -> None:
    wp = ItemsWritePolicy(derive=DeriveSpec(geometry_stats=[
        GeometryStat(stat=ComputedKind.AREA, store=None),
    ]))
    text = _run(wp, caplog)
    assert "store=None" in text
    assert "area" in text


def test_no_warning_when_stored(caplog) -> None:
    wp = ItemsWritePolicy(derive=DeriveSpec(geometry_stats=[
        GeometryStat(stat=ComputedKind.AREA, store=StatisticStorageMode.JSONB),
    ]))
    assert "store=None" not in _run(wp, caplog)


def test_no_warning_when_referenced_by_identity(caplog) -> None:
    # store=None feeding an identity rule is the legitimate compute-only case.
    wp = ItemsWritePolicy(
        derive=DeriveSpec(geometry_stats=[
            GeometryStat(stat=ComputedKind.AREA, store=None),
        ]),
        identity=[IdentityRule(match_on=["area"])],
    )
    assert "store=None" not in _run(wp, caplog)


def test_ignores_non_write_policy() -> None:
    # The handler is a no-op for any other config object (and must not raise).
    asyncio.run(_warn_unstored_unreferenced_stats(object(), "cat", "col", None))
