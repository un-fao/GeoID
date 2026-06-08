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

"""DWH join / feature export forces the full-precision PG read via Hint.JOIN.

The public default items READ routing puts Elasticsearch first, but ES only
carries simplified geometry and cannot run the ``ST_Transform`` projection the
DWH join builds into its request. The join/export paths pass ``{Hint.JOIN}`` so
driver resolution selects the PG read-primary; because PG advertises
``Capability.QUERY_FALLBACK_SOURCE``, ``_try_driver_dispatch`` declines (returns
``None``) and the inline PG SQL path serves the read. These tests pin that
wiring at the dispatch layer.
"""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.catalog import item_query as iq
from dynastore.modules.storage.hints import Hint


@pytest.mark.asyncio
async def test_try_driver_dispatch_forwards_join_hint_and_declines_to_pg_fallback():
    captured: dict = {}

    async def fake_get_driver(operation, catalog_id, collection_id=None, *, hints=frozenset()):
        captured["hints"] = hints
        # PG read-primary: advertises QUERY_FALLBACK_SOURCE.
        return SimpleNamespace(capabilities=frozenset({Capability.QUERY_FALLBACK_SOURCE}))

    with patch("dynastore.modules.storage.router.get_driver", new=fake_get_driver):
        result = await iq._try_driver_dispatch(
            "datamgr10", "region", "READ", None, 100, 0,
            hints=frozenset({Hint.JOIN}),
        )

    # The hint reached driver resolution …
    assert captured["hints"] == frozenset({Hint.JOIN})
    # … and the PG fallback makes dispatch decline so the inline PG SQL path runs.
    assert result is None


@pytest.mark.asyncio
async def test_try_driver_dispatch_default_hints_is_empty():
    # Without an explicit hint the dispatch resolves with empty hints, preserving
    # the public ES-first read default for every non-export caller.
    captured: dict = {}

    async def fake_get_driver(operation, catalog_id, collection_id=None, *, hints=frozenset()):
        captured["hints"] = hints
        return SimpleNamespace(capabilities=frozenset({Capability.QUERY_FALLBACK_SOURCE}))

    with patch("dynastore.modules.storage.router.get_driver", new=fake_get_driver):
        await iq._try_driver_dispatch("datamgr10", "region", "READ", None, 100, 0)

    assert captured["hints"] == frozenset()
