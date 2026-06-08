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
import os

@pytest.fixture(scope="session", autouse=True)
def gcp_env():
    """Sets environment variables required for GCP mocks."""
    os.environ["K_SERVICE"] = "test-service" # Require for GCP self-url resolution
    os.environ["SERVICE_URL"] = "https://example.com" # Fake real-world URL for topic subscription
    # os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project" # Now handled via GcpModuleConfig
