class TestDriverMeta:
    def test_class_name_matches_routing_contract(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        assert CollectionBigQueryDriver.__name__ == "CollectionBigQueryDriver"

    def test_capabilities_phase4a_set(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        d = CollectionBigQueryDriver()
        caps = d.capabilities
        assert "READ" in caps
        assert "STREAMING" in caps
        assert "INTROSPECTION" in caps
        assert "COUNT" in caps
        assert "AGGREGATION" in caps
        assert "WRITE" not in caps

    def test_preferred_for_features(self):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        d = CollectionBigQueryDriver()
        assert "features" in d.preferred_for

    def test_is_available_false_when_no_bq_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        monkeypatch.setattr(mod, "_get_bq_service", lambda: None)
        d = CollectionBigQueryDriver()
        assert d.is_available() is False
