import pytest

from dynastore.models.protocols.storage_driver import Capability


class TestCapabilityEnum:
    def test_standard_values(self):
        assert Capability.READ_ONLY == "read_only"
        assert Capability.STREAMING == "streaming"
        assert Capability.SPATIAL_FILTER == "spatial_filter"
        assert Capability.FULLTEXT == "fulltext"
        assert Capability.EXPORT == "export"
        assert Capability.TIME_TRAVEL == "time_travel"
        assert Capability.SOFT_DELETE == "soft_delete"
        assert Capability.VERSIONING == "versioning"
        assert Capability.SCHEMA_EVOLUTION == "schema_evolution"
        assert Capability.SNAPSHOTS == "snapshots"

    def test_is_str(self):
        assert isinstance(Capability.STREAMING, str)

    def test_frozenset_membership(self):
        caps = frozenset({Capability.STREAMING, Capability.EXPORT})
        assert Capability.STREAMING in caps
        assert Capability.READ_ONLY not in caps

    def test_mixed_with_custom_strings(self):
        caps = frozenset({Capability.STREAMING, "custom_capability"})
        assert Capability.STREAMING in caps
        assert "custom_capability" in caps
        assert "streaming" in caps

    def test_all_members(self):
        members = list(Capability)
        assert len(members) == 10
