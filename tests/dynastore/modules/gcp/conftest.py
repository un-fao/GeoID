import pytest


@pytest.fixture
def dynastore_modules(dynastore_modules):
    return dynastore_modules + ["gcp"]


@pytest.fixture
def dynastore_extensions(dynastore_extensions):
    return dynastore_extensions + ["gcp_bucket"]
