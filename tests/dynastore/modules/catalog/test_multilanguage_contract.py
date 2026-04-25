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

from tests.dynastore.test_utils import generate_test_id

import pytest
from dynastore.modules import get_protocol
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.models import Catalog, Collection

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xdist_group("catalog_lifespan"),
]


async def test_multilanguage_validation_conflicts(app_lifespan_module):
    """Tests that providing multilanguage dict with specific lang raises ValueError."""
    catalogs = get_protocol(CatalogsProtocol)

    catalog_id = f"val_conflict_{generate_test_id()}"

    # Clean up if exists
    await catalogs.delete_catalog(catalog_id, force=True)

    # 1. OK: single language with lang='en'
    data_ok = {"id": catalog_id, "title": "My Catalog"}
    await catalogs.create_catalog(data_ok, lang="en")

    # 2. FAIL: multilanguage dict with lang='en'
    data_fail = {"title": {"en": "Title", "it": "Titolo"}}
    with pytest.raises(ValueError, match="Conflicting language parameters"):
        await catalogs.update_catalog(catalog_id, data_fail, lang="en")

    # 3. OK: multilanguage dict with lang='*'
    data_multi = {"title": {"en": "Title", "it": "Titolo"}}
    updated = await catalogs.update_catalog(catalog_id, data_multi, lang="*")
    assert updated.title.en == "Title"
    assert updated.title.it == "Titolo"

    # Clean up
    await catalogs.delete_catalog(catalog_id, force=True)


async def test_convenience_methods(app_lifespan_module):
    """Tests create_catalog_localized and create_catalog_multilanguage."""
    catalogs = get_protocol(CatalogsProtocol)

    catalog_id = f"conv_methods_{generate_test_id()}"
    await catalogs.delete_catalog(catalog_id, force=True)

    # Localized create
    cat_loc = await catalogs.create_catalog(
        {"id": catalog_id, "title": "Italian Catalog"}, lang="it"
    )
    # The title on the model is a LocalizedText object
    assert cat_loc.title.it == "Italian Catalog"

    # Multilanguage create
    await catalogs.delete_catalog(catalog_id, force=True)
    cat_multi = await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": "English", "fr": "Français"}}, lang="*"
    )
    assert cat_multi.title.en == "English"
    assert cat_multi.title.fr == "Français"

    # Multilanguage get
    cat_get = await catalogs.get_catalog(catalog_id, lang="*")
    assert cat_get.title.en == "English"

    # Localized get
    cat_it = await catalogs.get_catalog(catalog_id, lang="it")
    # The returned model is a Catalog, title is LocalizedText.
    # Logic in get_catalog just returns the model.
    # The assertion below checks the CONTENT of the localized text.
    assert cat_it.title.en == "English"

    await catalogs.delete_catalog(catalog_id, force=True)


async def test_language_deletion(app_lifespan_module):
    """Tests delete_catalog_language and delete_collection_language."""
    catalogs = get_protocol(CatalogsProtocol)
    print(f"DEBUG: catalogs type: {type(catalogs)}")
    print(f"DEBUG: catalogs dir: {dir(catalogs)}")

    catalog_id = f"lang_del_{generate_test_id()}"
    await catalogs.delete_catalog(catalog_id, force=True)

    await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": "Title", "it": "Titolo", "fr": "Titre"}},
        lang="*",
    )

    # Delete 'it'
    success = await catalogs.delete_catalog_language(catalog_id, lang="it")
    assert success is True

    cat = await catalogs.get_catalog(catalog_id, lang="*")
    assert "it" not in cat.title.get_available_languages()
    assert "en" in cat.title.get_available_languages()

    # Try to delete last language (fail)
    await catalogs.delete_catalog_language(catalog_id, lang="fr")

    # Now only 'en' remains
    cat = await catalogs.get_catalog(catalog_id, lang="*")
    assert cat.title.get_available_languages() == {"en"}

    with pytest.raises(ValueError, match="is the only language available"):
        await catalogs.delete_catalog_language(catalog_id, lang="en")

    # Collection language deletion
    coll_id = "test_coll_lang"
    await catalogs.create_collection(
        catalog_id, {"id": coll_id, "title": {"en": "C-EN", "it": "C-IT"}}, lang="*"
    )

    await catalogs.delete_collection_language(catalog_id, coll_id, lang="it")
    coll = await catalogs.get_collection(catalog_id, coll_id, lang="*")
    assert "it" not in coll.title.get_available_languages()

    await catalogs.delete_catalog(catalog_id, force=True)


async def test_protocol_delegation(app_lifespan_module):
    """Tests that CatalogsProtocol correctly delegates to assets, configs, and localization."""
    catalogs = get_protocol(CatalogsProtocol)

    # 1. Assets delegation
    assert catalogs.assets is not None
    # Just check property works

    # 2. Configs delegation
    assert catalogs.configs is not None

    # 3. Localization delegation
    assert catalogs.localization is not None
    langs = catalogs.localization.get_supported_languages()
    assert "it" in langs
    assert catalogs.localization.is_multilanguage_input({"en": "val"}) is True
