import pytest

@pytest.fixture
def dynastore_extensions(dynastore_extensions):
    return dynastore_extensions + ["features"]
