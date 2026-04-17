import pytest
import pytest_asyncio


@pytest.fixture
def dynastore_extensions():
    """Enable 'features' so Phase 3 runs and installs the exposure matrix."""
    return ["features"]


@pytest_asyncio.fixture(loop_scope="function")
async def app_fixture(app_lifespan):
    """Return the FastAPI app instance after lifespan has executed."""
    yield app_lifespan.app
