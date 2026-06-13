from dynastore.extensions.stac.stac_contributor import LanguageStacContributor
from dynastore.models.localization import STAC_LANGUAGE_EXTENSION_URI
from dynastore.models.protocols.asset_contrib import ResourceRef


def _ref(available, lang="en"):
    return ResourceRef(
        catalog_id="c", collection_id="col", lang=lang,
        extras={"available_languages": available},
    )


def test_no_languages_yields_nothing():
    out = list(LanguageStacContributor().contribute_stac(_ref(set())))
    assert out == []


def test_single_language_declares_uri_and_language_no_languages_list():
    out = list(LanguageStacContributor().contribute_stac(_ref({"en"}, "en")))
    assert len(out) == 1
    contribution = out[0]
    assert STAC_LANGUAGE_EXTENSION_URI in contribution.stac_extensions
    assert "language" in contribution.extra_fields
    # only "en" available → no OTHER languages
    assert "languages" not in contribution.extra_fields


def test_multiple_languages_populate_languages_list():
    out = list(LanguageStacContributor().contribute_stac(_ref({"en", "fr"}, "en")))
    contribution = out[0]
    assert STAC_LANGUAGE_EXTENSION_URI in contribution.stac_extensions
    assert "language" in contribution.extra_fields
    assert "languages" in contribution.extra_fields
