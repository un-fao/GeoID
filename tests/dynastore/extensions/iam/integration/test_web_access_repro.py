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

import pytest
from httpx import AsyncClient

@pytest.fixture
def dynastore_extensions():
    # Ensure web extension is loaded
    return ["auth", "iam", "web", "features"]

@pytest.mark.asyncio
# @pytest.mark.skip()
async def test_public_web_access(in_process_client: AsyncClient):
    """Verify /web/ is accessible without credentials (public access policy)."""
    # Simply request the root web path
    response = await in_process_client.get("/web/")
    
    # 403 is the specific regression we want to avoid.
    # 200 = Success (index served)
    # 307/308 = Redirect (slash handling)
    # 404 = Static files not found (but access allowed)
    
    assert response.status_code != 403, f"Public web access forbidden: {response.text}"
    assert response.status_code in [200, 307, 308, 404]

@pytest.mark.asyncio
# @pytest.mark.skip()
async def test_web_admin_redirect(in_process_client: AsyncClient):
    """Verify /web/admin/ access behavior."""
    response = await in_process_client.get("/web/admin/")
    # Just ensure we don't crash with 500
    assert response.status_code != 500
