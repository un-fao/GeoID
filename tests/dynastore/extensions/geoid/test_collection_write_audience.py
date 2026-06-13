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

"""Unit tests for CollectionWriteAudience PluginConfig."""
from dynastore.extensions.geoid.configs import CollectionWriteAudience


def test_default_disallows_anonymous_create():
    cfg = CollectionWriteAudience()
    assert cfg.allow_anonymous_create is False


def test_allow_anonymous_create_can_be_set_true():
    cfg = CollectionWriteAudience(allow_anonymous_create=True)
    assert cfg.allow_anonymous_create is True


def test_address_is_platform_catalog_collection_write_audience():
    assert CollectionWriteAudience._address == (
        "platform", "catalog", "collection", "write_audience",
    )


def test_freeze_at_is_collection_tier():
    assert CollectionWriteAudience._freeze_at == "collection"


def test_class_key_is_snake_case():
    assert CollectionWriteAudience.class_key() == "collection_write_audience"
