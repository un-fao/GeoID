"""Platform notebook registrations for the GCP module."""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.gcp"


register_platform_notebook(
    notebook_id="gcp_bucket_init_upload_and_ingest",
    registered_by=_REG,
    notebook_path=_HERE / "gc01_bucket_init_upload_and_ingest.ipynb",
    title={"en": "GCP — Bucket Init, Upload, Ingest"},
    description={"en": "Provision a tenant bucket, upload an asset, and ingest it into a collection."},
    tags=["gcp", "bucket", "upload", "ingest"],
)
