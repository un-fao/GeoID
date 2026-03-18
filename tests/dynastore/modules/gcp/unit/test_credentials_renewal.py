import pytest
from unittest.mock import MagicMock, patch
from dynastore.modules.gcp.gcp_module import GCPModule

@pytest.fixture
def mock_app_state():
    state = MagicMock()
    state.engine = None
    return state

@pytest.fixture
def mock_credentials():
    creds = MagicMock()
    creds.valid = True
    creds.expired = False
    creds.token = "initial-token"
    return creds

@pytest.mark.asyncio
async def test_get_fresh_token_async(mock_app_state, mock_credentials):
    with patch("dynastore.modules.gcp.gcp_module.get_credentials", return_value=(mock_credentials, {"project_id": "test-project"})):
        with patch("google.cloud.storage.Client"), \
             patch("google.cloud.pubsub_v1.PublisherClient"), \
             patch("google.cloud.pubsub_v1.SubscriberClient"):
            
            gcp = GCPModule(mock_app_state)
            
            # Mock concurrency backend to run synchronously for test
            async def mock_run_async(f, *args, **kwargs):
                return f(*args, **kwargs)

            mock_credentials.valid = False
            mock_credentials.expired = True
            mock_credentials.token = "refreshed-token"
                
            token = await gcp.get_fresh_token()
            assert token == "refreshed-token"
            mock_credentials.refresh.assert_called_once()
