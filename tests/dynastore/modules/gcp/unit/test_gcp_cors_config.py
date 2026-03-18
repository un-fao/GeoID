import pytest
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig, GcpCorsRule

def test_gcp_cors_rule_alias():
    """Verify that response_header is correctly aliased to responseHeader."""
    rule = GcpCorsRule(
        origin=["*"],
        method=["GET"],
        response_header=["Content-Type"],
        max_age_seconds=3600
    )
    dump = rule.model_dump(by_alias=True)
    assert dump["origin"] == ["*"]
    assert dump["method"] == ["GET"]
    assert dump["responseHeader"] == ["Content-Type"]
    assert dump["max_age_seconds"] == 3600

def test_gcp_catalog_bucket_config_serialization():
    """Verify that GcpCatalogBucketConfig includes CORS and serializes correctly."""
    config = GcpCatalogBucketConfig(
        cors=[
            GcpCorsRule(origin=["https://example.com"], method=["GET", "POST"])
        ]
    )
    dump = config.model_dump()
    assert len(dump["cors"]) == 1
    assert dump["cors"][0]["origin"] == ["https://example.com"]
    assert dump["cors"][0]["method"] == ["GET", "POST"]

def test_gcp_catalog_bucket_config_default_cors():
    """Verify that GcpCatalogBucketConfig has CORS to * by default."""
    config = GcpCatalogBucketConfig()
    assert len(config.cors) == 1
    assert config.cors[0].origin == ["*"]
    assert "GET" in config.cors[0].method
    assert config.cors[0].response_header == ["*"]
