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

import pytest
from pydantic import ValidationError

from dynastore.extensions.volumes.config import VolumesConfig


def test_defaults():
    cfg = VolumesConfig()
    assert cfg.max_features_per_tile == 10_000
    assert cfg.max_tree_depth == 20
    assert cfg.on_demand_cache_ttl_s == 3600
    assert cfg.default_height_attr == "height"
    assert cfg.root_geometric_error == 500.0
    assert cfg.refinement_ratio == 4.0


def test_rejects_negative_depth():
    with pytest.raises(ValidationError):
        VolumesConfig(max_tree_depth=-1)


def test_rejects_non_positive_error():
    with pytest.raises(ValidationError):
        VolumesConfig(root_geometric_error=0)


def test_override_values():
    cfg = VolumesConfig(max_features_per_tile=50, max_tree_depth=3)
    assert cfg.max_features_per_tile == 50
    assert cfg.max_tree_depth == 3
