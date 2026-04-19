import pytest
from pydantic import ValidationError

from dynastore.modules.storage.drivers.bigquery_models import (
    BigQueryTarget,
    CollectionBigQueryDriverConfig,
)


def test_target_all_fields_optional():
    BigQueryTarget()
    t = BigQueryTarget(project_id="p", dataset_id="d", table_name="t")
    assert t.project_id == "p"


def test_target_rejects_unknown_field():
    with pytest.raises(ValidationError):
        BigQueryTarget(bogus_field="x")  # type: ignore[call-arg]


def test_target_is_fully_qualified():
    t = BigQueryTarget(project_id="p", dataset_id="d", table_name="t")
    assert t.is_fully_qualified()
    assert not BigQueryTarget(project_id="p").is_fully_qualified()


def test_target_fqn():
    t = BigQueryTarget(project_id="p", dataset_id="d", table_name="t")
    assert t.fqn() == "p.d.t"
    with pytest.raises(ValueError):
        BigQueryTarget(project_id="p").fqn()


def test_driver_config_defaults():
    cfg = CollectionBigQueryDriverConfig()
    assert cfg.target.project_id is None
    assert cfg.location == "EU"
    assert cfg.page_size == 1000
    assert cfg.query_timeout_s == 60
