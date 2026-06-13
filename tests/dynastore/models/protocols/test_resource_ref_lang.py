from dynastore.models.protocols.asset_contrib import ResourceRef


def test_resource_ref_lang_defaults_none():
    ref = ResourceRef(catalog_id="c", collection_id="col")
    assert ref.lang is None


def test_resource_ref_lang_set():
    ref = ResourceRef(catalog_id="c", collection_id="col", lang="fr")
    assert ref.lang == "fr"
