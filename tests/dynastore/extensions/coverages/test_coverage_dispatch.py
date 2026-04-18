import pytest


def test_format_param_resolves_default():
    from dynastore.extensions.coverages.coverages_service import _resolve_format
    assert _resolve_format("geotiff") == "geotiff"
    assert _resolve_format("GeoTIFF") == "geotiff"
    assert _resolve_format(None) == "geotiff"


def test_format_param_rejects_unknown():
    from fastapi import HTTPException
    from dynastore.extensions.coverages.coverages_service import _resolve_format
    with pytest.raises(HTTPException) as exc:
        _resolve_format("webp")
    assert exc.value.status_code == 415
