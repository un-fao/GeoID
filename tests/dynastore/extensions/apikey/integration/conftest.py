import pytest
import os

@pytest.fixture
def dynastore_modules():
    return ["db_config", "db", "catalog", "stats", "apikey"]

@pytest.fixture
def dynastore_extensions():
    return ["auth", "apikey", "features"]
