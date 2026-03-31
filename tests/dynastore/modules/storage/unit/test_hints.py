import pytest

from dynastore.modules.storage.hints import (
    ReadHint,
    register_hint,
    get_registered_hints,
    _CUSTOM_HINTS,
)


class TestReadHintEnum:
    def test_standard_values(self):
        assert ReadHint.DEFAULT == "default"
        assert ReadHint.SEARCH == "search"
        assert ReadHint.FEATURES == "features"
        assert ReadHint.GRAPH == "graph"
        assert ReadHint.ANALYTICS == "analytics"
        assert ReadHint.CACHE == "cache"

    def test_is_str(self):
        assert isinstance(ReadHint.DEFAULT, str)
        assert ReadHint.DEFAULT == "default"

    def test_all_members(self):
        members = list(ReadHint)
        assert len(members) == 8


class TestCustomHintRegistration:
    def setup_method(self):
        _CUSTOM_HINTS.clear()

    def teardown_method(self):
        _CUSTOM_HINTS.clear()

    def test_register_custom_hint(self):
        register_hint("realtime", "Real-time streaming reads")
        hints = get_registered_hints()
        assert "realtime" in hints
        assert hints["realtime"] == "Real-time streaming reads"

    def test_register_multiple_hints(self):
        register_hint("realtime", "Real-time")
        register_hint("archive", "Archive reads")
        hints = get_registered_hints()
        assert "realtime" in hints
        assert "archive" in hints

    def test_standard_hints_always_present(self):
        hints = get_registered_hints()
        for h in ReadHint:
            assert h.value in hints

    def test_custom_hints_merged_with_standard(self):
        register_hint("custom1", "Custom hint")
        hints = get_registered_hints()
        assert len(hints) == len(ReadHint) + 1

    def test_overwrite_custom_hint(self):
        register_hint("custom1", "V1")
        register_hint("custom1", "V2")
        hints = get_registered_hints()
        assert hints["custom1"] == "V2"
