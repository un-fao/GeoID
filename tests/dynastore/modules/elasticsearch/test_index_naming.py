"""Naming contract for ES index helpers — Phase 2 target topology.

Target shape (user-specified):

  - ``dynastore-catalogs``                  singleton, full catalog object
  - ``dynastore-collections``               singleton, full collection object
  - ``dynastore-items``                     alias spanning all per-catalog public items indexes
  - ``dynastore-{catalog}-items``           per-catalog public items index
  - ``dynastore-{catalog}-private-items``   per-catalog private items index (NOT in public alias)
"""

from dynastore.modules.elasticsearch.mappings import (
    DYNAMIC_TEMPLATES,
    get_index_name,
    get_public_items_alias,
    get_search_index,
    get_tenant_items_index,
)
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    get_private_index_name,
)


PREFIX = "dynastore"


def test_catalogs_singleton_unchanged():
    assert get_index_name(PREFIX, "catalog") == "dynastore-catalogs"


def test_collections_singleton_unchanged():
    assert get_index_name(PREFIX, "collection") == "dynastore-collections"


def test_per_catalog_items_index_uses_catalog_first_naming():
    assert get_tenant_items_index(PREFIX, "adm2_catalog") == "dynastore-adm2_catalog-items"


def test_public_items_alias_drops_public_suffix():
    assert get_public_items_alias(PREFIX) == "dynastore-items"


def test_private_items_uses_catalog_first_naming():
    assert get_private_index_name(PREFIX, "adm2_catalog") == "dynastore-adm2_catalog-private-items"


def test_search_items_scoped_targets_per_catalog_index():
    assert get_search_index(PREFIX, "item", "adm2_catalog") == "dynastore-adm2_catalog-items"


def test_search_items_unscoped_targets_alias():
    assert get_search_index(PREFIX, "item", None) == "dynastore-items"


def test_search_collections_scoped_targets_singleton():
    # Collections move to a singleton — scoped search filters via _routing,
    # not via separate per-catalog indexes.
    assert get_search_index(PREFIX, "collection", "adm2_catalog") == "dynastore-collections"


def test_search_collections_unscoped_targets_singleton():
    assert get_search_index(PREFIX, "collection", None) == "dynastore-collections"


def test_search_catalogs_targets_singleton():
    assert get_search_index(PREFIX, "catalog", None) == "dynastore-catalogs"


def _find_template(name: str) -> dict:
    for entry in DYNAMIC_TEMPLATES:
        if name in entry:
            return entry[name]
    raise AssertionError(f"dynamic template {name!r} not found")


def test_dynamic_templates_apply_per_language_analyzer_to_titles():
    # Each supported locale gets its own dynamic template so titles are
    # tokenised/stemmed in the right language.
    expected_analyzers = {
        "en": "english",
        "fr": "french",
        "es": "spanish",
        "ru": "russian",
        "ar": "arabic",
        "it": "italian",
        "de": "german",
    }
    for lang, analyzer in expected_analyzers.items():
        tmpl = _find_template(f"title_{lang}")
        assert tmpl["path_match"] == f"*title.{lang}"
        assert tmpl["mapping"]["analyzer"] == analyzer
        assert tmpl["mapping"]["type"] == "text"


def test_dynamic_templates_apply_per_language_analyzer_to_descriptions():
    tmpl = _find_template("description_fr")
    assert tmpl["path_match"] == "*description.fr"
    assert tmpl["mapping"]["analyzer"] == "french"


def test_zh_falls_back_to_standard_analyzer():
    # ES has no built-in Chinese analyzer; we use 'standard' until smartcn/ICU
    # is added at the cluster level. Documented choice, not an oversight.
    tmpl = _find_template("title_zh")
    assert tmpl["mapping"]["analyzer"] == "standard"
