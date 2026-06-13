#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""#1212 — geoid is the feature id by default; created is opt-in; expose_geoid gone.

Product decisions:
  1. ``feature.id`` defaults to the internal ``geoid``. A collection may opt
     into using ``external_id`` as the id via
     ``ItemsReadPolicy.feature_type.external_id_as_feature_id = True``.
  2. ``expose_geoid`` is removed from the attributes sidecar config outright
     (no deprecation / no back-compat) — geoid exposure is a read-policy
     concern, not a write-storage knob.
  3. ``created`` (the storage ``transaction_time``) is NOT injected into
     ``properties`` by default; it is surfaced only when the read policy's
     ``feature_type.expose_created`` is set. By default the read response is the
     input feature 1:1, with only the id replaced by the geoid.
"""

from datetime import datetime, timezone

from geojson_pydantic import Feature

from dynastore.modules.storage.computed_fields import FeatureType
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy

_GEOID = "11111111-1111-1111-1111-111111111111"
_EXTID = "item1_country2_may22"


def _feature() -> Feature:
    # Hub initialises feature.id = geoid before the attributes sidecar runs.
    return Feature(type="Feature", geometry=None, properties={}, id=_GEOID)


def _row() -> dict:
    return {
        "geoid": _GEOID,
        "external_id": _EXTID,
        "transaction_time": datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc),
    }


def _ctx(read_policy=None) -> FeaturePipelineContext:
    ctx = FeaturePipelineContext()
    if read_policy is not None:
        ctx["_items_read_policy"] = read_policy
    return ctx


# ---------------------------------------------------------------------------
# Decision 1 — geoid is the default id
# ---------------------------------------------------------------------------

def test_feature_type_default_id_is_geoid():
    assert FeatureType().external_id_as_feature_id is False


def test_map_row_keeps_geoid_id_by_default():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    sidecar.map_row_to_feature(_row(), feature, _ctx())
    assert feature.id == _GEOID


def test_map_row_uses_external_id_when_policy_opts_in():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    policy = ItemsReadPolicy(
        feature_type=FeatureType(external_id_as_feature_id=True)
    )
    sidecar.map_row_to_feature(_row(), feature, _ctx(policy))
    assert feature.id == _EXTID


# ---------------------------------------------------------------------------
# Decision 2 — expose_geoid is gone
# ---------------------------------------------------------------------------

def test_sidecar_config_has_no_expose_geoid_field():
    assert "expose_geoid" not in FeatureAttributeSidecarConfig.model_fields


# ---------------------------------------------------------------------------
# Decision 3 — created is opt-in via feature_type
# ---------------------------------------------------------------------------

def test_feature_type_default_does_not_expose_created():
    assert FeatureType().expose_created is False


def test_map_row_omits_created_by_default():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    sidecar.map_row_to_feature(_row(), feature, _ctx())
    assert "created" not in (feature.properties or {})


def test_map_row_exposes_created_when_policy_opts_in():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    policy = ItemsReadPolicy(feature_type=FeatureType(expose_created=True))
    sidecar.map_row_to_feature(_row(), feature, _ctx(policy))
    assert (feature.properties or {}).get("created") == "2026-05-22T12:00:00Z"


# ---------------------------------------------------------------------------
# Refinement — optional geoid property, surfaced only when id == external_id
# ---------------------------------------------------------------------------

def test_feature_type_default_does_not_expose_geoid_property():
    assert FeatureType().expose_geoid is False


def test_map_row_exposes_geoid_property_when_external_id_is_id():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    policy = ItemsReadPolicy(
        feature_type=FeatureType(
            external_id_as_feature_id=True, expose_geoid=True
        )
    )
    sidecar.map_row_to_feature(_row(), feature, _ctx(policy))
    # id is the external_id, and the geoid is preserved as a property
    assert feature.id == _EXTID
    assert (feature.properties or {}).get("geoid") == _GEOID


def test_map_row_geoid_property_is_noop_when_id_already_geoid():
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    feature = _feature()
    # expose_geoid set, but id stays geoid (external_id_as_feature_id=False)
    policy = ItemsReadPolicy(feature_type=FeatureType(expose_geoid=True))
    sidecar.map_row_to_feature(_row(), feature, _ctx(policy))
    assert feature.id == _GEOID
    assert "geoid" not in (feature.properties or {})
