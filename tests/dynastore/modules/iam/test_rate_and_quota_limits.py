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

"""
Tests for rate limiting and quota enforcement.

The API key usage-counter backend was removed in Phase 1 of the IAM refactor.
Rate limit and quota condition handlers are currently no-op stubs.
These tests will be restored when a new rate-limiting backend (e.g. Redis/Valkey)
is introduced.
"""

import pytest


@pytest.mark.skip(reason="Rate-limit/quota storage backend removed in Phase 1 IAM refactor")
class TestRateAndQuotaLimits:
    """Placeholder — re-enable when a new rate-limiting backend is available."""

    async def test_rate_limit_enforcement(self):
        pass

    async def test_quota_limit_enforcement(self):
        pass
