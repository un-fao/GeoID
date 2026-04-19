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


def test_credentials_default_is_empty():
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
        CollectionBigQueryDriverConfig,
    )
    creds = BigQueryCredentials()
    assert creds.is_empty()
    cfg = CollectionBigQueryDriverConfig()
    assert cfg.credentials.is_empty()


def test_credentials_accept_secret_wrapped_sa_json():
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
    )
    from dynastore.tools.secrets import Secret

    creds = BigQueryCredentials(service_account_json=Secret("{...}"))
    assert not creds.is_empty()
    assert creds.service_account_json is not None
    # Reveal preserves the plaintext, __str__ masks it.
    assert creds.service_account_json.reveal() == "{...}"
    assert "***" in str(creds.service_account_json)


def test_credentials_accept_plain_string_coerced_to_secret():
    """Pydantic pre-validator in Secret should coerce plain str -> Secret."""
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
    )
    creds = BigQueryCredentials(service_account_json="raw-json-text")
    assert creds.service_account_json is not None
    # str -> Secret via Pydantic validator; reveal() works.
    assert creds.service_account_json.reveal() == "raw-json-text"


def test_credentials_rejects_extra_fields():
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
    )
    with pytest.raises(ValidationError):
        BigQueryCredentials(bogus="x")  # type: ignore[call-arg]


