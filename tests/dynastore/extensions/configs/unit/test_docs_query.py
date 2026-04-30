"""Tests for the ``?docs=`` query mode on the composed-config endpoints.

The flag is orthogonal to ``?meta=true``:

  * ``?docs=none`` (default) — no schemas in the response; backward-compat.
  * ``?docs=schema``         — each class in the response gets its full
                               JSON Schema 2020-12 document at
                               ``meta.{ClassName}.json_schema``.

When ``?docs=schema`` is requested without ``?meta=true``, a meta block is
still emitted so the schemas have somewhere to live; ``source`` falls back
to ``sources[class_key]`` (or ``"default"`` when the class isn't in
sources, e.g. ``resolved=False``). ``layers`` stays null in that path.
"""

from unittest.mock import patch

from dynastore.extensions.configs.config_api_service import ConfigApiService


def _stub_registry_with_schema(**classes):
    """Build a fake registry where each class also exposes a callable
    ``model_json_schema()`` returning a deterministic stub document.

    Mirrors the production contract: every concrete ``PluginConfig`` is a
    Pydantic v2 model and `model_json_schema()` returns a dict with at
    minimum ``title`` and ``type``.
    """
    out = {}
    for name, attrs in classes.items():
        body = {}
        if attrs.get("abstract"):
            body["is_abstract_base"] = True
        if "_address" in attrs:
            body["_address"] = attrs["_address"]
        if "_visibility" in attrs:
            body["_visibility"] = attrs["_visibility"]
        # Closure capture: each class returns its own stub schema
        body["model_json_schema"] = classmethod(
            lambda cls, _name=name: {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": _name,
                "type": "object",
                "properties": {"stub": {"type": "string"}},
            }
        )
        cls = type(name, (), body)
        cls.__module__ = attrs.get("__module__", "test.stub")
        out[name] = cls
    return out


# ---------------------------------------------------------------------------
# include_schemas=False (default) — no schema embedded, no meta unless asked
# ---------------------------------------------------------------------------

def test_compose_tree_no_schemas_when_include_schemas_false_and_meta_off():
    by_class = {"WebConfig": {"brand_name": "x"}}
    registry = _stub_registry_with_schema(
        WebConfig={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={"WebConfig": "platform"}, active_scope="platform",
            include_meta=False, include_schemas=False,
        )
    assert tree == {"platform": {"web": {"WebConfig": {"brand_name": "x"}}}}
    assert meta is None


def test_compose_tree_meta_only_no_schema_when_only_meta_requested():
    by_class = {"WebConfig": {"brand_name": "x"}}
    registry = _stub_registry_with_schema(
        WebConfig={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={"WebConfig": "platform"}, active_scope="platform",
            include_meta=True, include_schemas=False,
        )
    assert meta is not None and "WebConfig" in meta
    assert meta["WebConfig"].source == "platform"
    assert meta["WebConfig"].json_schema is None


# ---------------------------------------------------------------------------
# include_schemas=True — schema embedded per class
# ---------------------------------------------------------------------------

def test_compose_tree_schema_attached_when_include_schemas_true():
    by_class = {"WebConfig": {"brand_name": "x"}}
    registry = _stub_registry_with_schema(
        WebConfig={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={"WebConfig": "platform"}, active_scope="platform",
            include_meta=False, include_schemas=True,
        )
    # Tree shape unchanged (data and schema are kept separate)
    assert tree == {"platform": {"web": {"WebConfig": {"brand_name": "x"}}}}
    # Meta dict materialises specifically to carry the schema
    assert meta is not None and "WebConfig" in meta
    assert meta["WebConfig"].source == "platform"
    assert meta["WebConfig"].layers is None
    assert meta["WebConfig"].json_schema == {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "WebConfig",
        "type": "object",
        "properties": {"stub": {"type": "string"}},
    }


def test_compose_tree_schema_falls_back_to_default_source_when_class_not_in_sources():
    """``?docs=schema`` should still attach schemas even when ``resolved=False``
    leaves a class out of ``sources`` — source falls back to "default" so the
    meta entry has somewhere to live.
    """
    by_class = {"WebConfig": {"brand_name": "x"}}
    registry = _stub_registry_with_schema(
        WebConfig={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform",
            include_meta=False, include_schemas=True,
        )
    assert meta is not None and "WebConfig" in meta
    assert meta["WebConfig"].source == "default"
    assert meta["WebConfig"].json_schema is not None


def test_compose_tree_meta_and_schema_combine():
    """``?meta=true&?docs=schema`` produces both diagnostics and schema in
    the same meta entry (orthogonal flags).
    """
    by_class = {"WebConfig": {"brand_name": "x"}}
    registry = _stub_registry_with_schema(
        WebConfig={"_address": ("platform", "web", None)},
    )
    tier_data = {
        "platform": {"WebConfig": {"brand_name": "x"}},
        "catalog": {},
        "collection": {},
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={"WebConfig": "platform"}, active_scope="platform",
            include_meta=True, include_schemas=True, tier_data=tier_data,
        )
    assert meta is not None and "WebConfig" in meta
    entry = meta["WebConfig"]
    assert entry.source == "platform"
    assert entry.layers is not None  # waterfall trace populated by include_meta
    assert entry.json_schema is not None  # schema populated by include_schemas
    assert entry.json_schema["title"] == "WebConfig"


# ---------------------------------------------------------------------------
# Schema attached for inherited_from_catalog block too (collection scope)
# ---------------------------------------------------------------------------

def test_compose_tree_schema_attached_for_inherited_from_catalog_block():
    """Catalog-visibility configs surfaced under ``inherited_from_catalog`` at
    collection scope should also get their schema when ``include_schemas=True``.
    """
    by_class = {"CatalogOnly": {"private": True}}
    registry = _stub_registry_with_schema(
        CatalogOnly={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={"CatalogOnly": "catalog"}, active_scope="collection",
            include_meta=False, include_schemas=True,
        )
    # Class lands under inherited_from_catalog at collection scope
    assert "inherited_from_catalog" in tree
    assert "CatalogOnly" in tree["inherited_from_catalog"]["storage"]["drivers"]["catalog"]
    # Schema still attaches via meta
    assert meta is not None and "CatalogOnly" in meta
    assert meta["CatalogOnly"].json_schema is not None
    assert meta["CatalogOnly"].json_schema["title"] == "CatalogOnly"
