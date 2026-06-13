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

"""CacheModule graceful-skip when the ``module_cache`` extra is absent.

Sync-only worker SCOPEs (e.g. the ingestion job) still have ``VALKEY_URL``
set in the environment but do not pip-install msgpack/valkey. The module
must skip its lifespan cleanly instead of letting
``ValkeyCacheBackend.__init__`` raise — a plain ``ImportError`` escaping
the lifespan would crash the worker on startup.
"""

from __future__ import annotations

from dynastore.modules.cache.cache_module import CacheModule


async def test_lifespan_skips_cleanly_when_deps_absent(monkeypatch):
    """VALKEY_URL set + deps missing → lifespan yields, no exception."""
    monkeypatch.setenv("VALKEY_URL", "valkey://10.0.0.1:6379")
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", False)

    module = CacheModule(app_state=object())
    entered = False
    async with module.lifespan(object()):
        entered = True
    assert entered  # got past `yield` without raising


async def test_lifespan_skips_when_valkey_url_unset(monkeypatch):
    """No VALKEY_URL at all → local cache, lifespan yields cleanly."""
    monkeypatch.delenv("VALKEY_URL", raising=False)

    module = CacheModule(app_state=object())
    entered = False
    async with module.lifespan(object()):
        entered = True
    assert entered
