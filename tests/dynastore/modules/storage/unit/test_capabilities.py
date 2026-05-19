import pytest

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.hints import Hint


class TestCapabilityEnum:
    def test_standard_values(self):
        assert Capability.READ_ONLY == "read_only"
        assert Capability.STREAMING == "streaming"
        assert Capability.EXPORT == "export"
        assert Capability.TIME_TRAVEL == "time_travel"
        assert Capability.SOFT_DELETE == "soft_delete"
        assert Capability.VERSIONING == "versioning"
        assert Capability.SCHEMA_EVOLUTION == "schema_evolution"
        assert Capability.SNAPSHOTS == "snapshots"

    def test_read_flavours_moved_to_hint(self):
        """Read-flavour capabilities (FULLTEXT, SPATIAL_FILTER, AGGREGATION,
        COUNT, STATISTICS, SORT, GROUP_BY, ATTRIBUTE_FILTER) were retired
        from ``Capability`` in PR #3b and moved to the ``Hint``
        catalogue.  They're per-request preferences, not structural facts.
        """
        for moved in (
            "FULLTEXT", "SPATIAL_FILTER", "ATTRIBUTE_FILTER",
            "AGGREGATION", "COUNT", "STATISTICS", "SORT", "GROUP_BY",
        ):
            assert not hasattr(Capability, moved), (
                f"Capability.{moved} should have moved to Hint catalogue"
            )

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
        # Capability is a plain class (not Enum) — collect string constants via inspection
        import inspect
        members = [
            v for k, v in inspect.getmembers(Capability)
            if not k.startswith("_") and isinstance(v, str)
        ]
        assert len(members) >= 10


class TestReadFlavoursOnHintEnum:
    """Mirror of ``test_read_flavours_moved_to_hint``: pin that the eight
    relocated read-flavours are present on the ``Hint`` catalogue.

    Together with the negative pin on ``Capability`` this fences the
    structural-vs-per-request axis split: a future refactor that adds
    one of these back to ``Capability`` (or drops it from ``Hint``)
    fails one of these two tests immediately.
    """

    @pytest.mark.parametrize("name", [
        "FULLTEXT", "SPATIAL_FILTER", "ATTRIBUTE_FILTER",
        "AGGREGATION", "COUNT", "STATISTICS", "SORT", "GROUP_BY",
    ])
    def test_read_flavour_lives_on_hint(self, name: str) -> None:
        assert hasattr(Hint, name), (
            f"Hint.{name} should be the canonical home for the per-request "
            "read-variant preference."
        )


class TestBigQueryHintRetired:
    """``Hint.BIGQUERY`` was a legacy driver self-tag, not a per-request
    preference.  Driver-class identity now lives on
    ``ItemsBigQueryDriver.backend_id``; the request-side ``Hint``
    vocabulary stays clean.
    """

    def test_hint_bigquery_member_removed(self) -> None:
        assert not hasattr(Hint, "BIGQUERY"), (
            "Hint.BIGQUERY was removed — use ItemsBigQueryDriver.backend_id "
            "for structural backend identity instead."
        )

    def test_bq_driver_advertises_backend_id(self) -> None:
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        assert ItemsBigQueryDriver.backend_id == "bigquery"

    def test_bq_driver_supported_hints_excludes_self_tag(self) -> None:
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        assert "bigquery" not in {str(h) for h in ItemsBigQueryDriver.supported_hints}
