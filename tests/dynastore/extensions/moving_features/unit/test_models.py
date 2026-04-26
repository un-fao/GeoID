"""Unit tests for modules/moving_features/models.py — Pydantic model validation."""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dynastore.modules.moving_features.models import (
    InterpolationEnum,
    MovingFeature,
    MovingFeatureCreate,
    TemporalGeometry,
    TemporalGeometryCreate,
)


# ---------------------------------------------------------------------------
# MovingFeatureCreate
# ---------------------------------------------------------------------------

def test_mf_create_defaults():
    mf = MovingFeatureCreate()
    assert mf.feature_type == "Feature"
    assert mf.properties is None


def test_mf_create_with_properties():
    mf = MovingFeatureCreate(feature_type="Feature", properties={"name": "truck-001", "fleet": "A"})
    assert mf.properties["name"] == "truck-001"


def test_mf_create_custom_type():
    mf = MovingFeatureCreate(feature_type="Vehicle")
    assert mf.feature_type == "Vehicle"


# ---------------------------------------------------------------------------
# MovingFeature (full DB record)
# ---------------------------------------------------------------------------

def test_moving_feature_valid():
    mf = MovingFeature(
        id=uuid.uuid4(),
        catalog_id="test-catalog",
        collection_id="fleet",
        feature_type="Feature",
        properties={"driver": "Alice"},
    )
    assert mf.catalog_id == "test-catalog"
    assert mf.collection_id == "fleet"
    assert isinstance(mf.created_at, datetime)


# ---------------------------------------------------------------------------
# TemporalGeometryCreate
# ---------------------------------------------------------------------------

def test_tg_create_valid():
    now = datetime.now(timezone.utc)
    later = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    tg = TemporalGeometryCreate(
        datetimes=[now, later],
        coordinates=[[10.0, 52.0], [11.0, 53.0]],
    )
    assert tg.interpolation == InterpolationEnum.LINEAR
    assert tg.crs == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    assert len(tg.datetimes) == 2
    assert len(tg.coordinates) == 2


def test_tg_create_empty_datetimes_raises():
    with pytest.raises(ValidationError):
        TemporalGeometryCreate(datetimes=[], coordinates=[])


def test_tg_create_3d_coordinates():
    now = datetime.now(timezone.utc)
    tg = TemporalGeometryCreate(
        datetimes=[now],
        coordinates=[[10.0, 52.0, 100.0]],
    )
    assert tg.coordinates[0][2] == 100.0


def test_tg_create_custom_interpolation():
    now = datetime.now(timezone.utc)
    tg = TemporalGeometryCreate(
        datetimes=[now],
        coordinates=[[0.0, 0.0]],
        interpolation=InterpolationEnum.STEP,
    )
    assert tg.interpolation == InterpolationEnum.STEP


def test_tg_create_with_properties():
    now = datetime.now(timezone.utc)
    tg = TemporalGeometryCreate(
        datetimes=[now],
        coordinates=[[0.0, 0.0]],
        properties={"speed": 80.5, "heading": 270},
    )
    assert tg.properties["speed"] == 80.5


# ---------------------------------------------------------------------------
# TemporalGeometry (full DB record)
# ---------------------------------------------------------------------------

def test_temporal_geometry_valid():
    mf_id = uuid.uuid4()
    tg = TemporalGeometry(
        id=uuid.uuid4(),
        mf_id=mf_id,
        catalog_id="test-catalog",
        datetimes=[datetime.now(timezone.utc)],
        coordinates=[[10.0, 52.0]],
    )
    assert tg.mf_id == mf_id
    assert isinstance(tg.created_at, datetime)


# ---------------------------------------------------------------------------
# InterpolationEnum
# ---------------------------------------------------------------------------

def test_interpolation_enum_values():
    assert InterpolationEnum.LINEAR.value == "Linear"
    assert InterpolationEnum.STEP.value == "Step"
    assert InterpolationEnum.QUADRATIC.value == "Quadratic"
    assert InterpolationEnum.CUBIC.value == "Cubic"
