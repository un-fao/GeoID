import pytest
import os

@pytest.fixture(scope="session", autouse=True)
def gcp_env():
    """Sets environment variables required for GCP mocks."""
    os.environ["K_SERVICE"] = "test-service" # Require for GCP self-url resolution
    os.environ["SERVICE_URL"] = "https://example.com" # Fake real-world URL for topic subscription
    # os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project" # Now handled via GcpModuleConfig
