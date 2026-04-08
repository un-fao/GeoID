import os


def generate_test_id(length: int = 8) -> str:
    """Short random hex ID for tests. Uses os.urandom to avoid UUIDv7 timestamp collisions under xdist."""
    return os.urandom((length + 1) // 2).hex()[:length]
