import pytest
from httpx import AsyncClient

@pytest.fixture
def dynastore_extensions():
    # Ensure web extension is loaded
    return ["auth", "apikey", "web", "features"]

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
