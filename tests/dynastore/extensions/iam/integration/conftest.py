import pytest
import os

@pytest.fixture
def dynastore_modules():
    return ["db_config", "db", "catalog", "stats", "iam"]

@pytest.fixture
def dynastore_extensions():
    return ["auth", "iam", "features"]
