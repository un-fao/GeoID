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
    # Patch paths must target the module-level references (already imported by gcp_module.py)
    with patch("dynastore.modules.gcp.gcp_module.get_credentials", return_value=(mock_credentials, {"project_id": "test-project"})), \
         patch("dynastore.modules.gcp.gcp_module.storage") as mock_storage, \
         patch("dynastore.modules.gcp.gcp_module.pubsub_v1") as mock_pubsub:

        mock_storage.Client.return_value = MagicMock()
        mock_pubsub.PublisherClient.return_value = MagicMock()
        mock_pubsub.SubscriberClient.return_value = MagicMock()

        gcp = GCPModule(mock_app_state)

        # Credentials must be set after successful construction
        assert gcp._credentials is mock_credentials, (
            "GCPModule did not store credentials. Check that reinitialize_clients() did not raise."
        )

        mock_credentials.valid = False
        mock_credentials.expired = True
        mock_credentials.token = "refreshed-token"

        token = await gcp.get_fresh_token()
        assert token == "refreshed-token"
        mock_credentials.refresh.assert_called_once()
