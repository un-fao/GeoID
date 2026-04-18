from dynastore.modules.styles.encodings import (
    MEDIA_TYPE_FLAT_STYLE,
    MEDIA_TYPE_HTML,
    MEDIA_TYPE_MAPBOX_GL,
    MEDIA_TYPE_QML,
    MEDIA_TYPE_SLD_10,
    MEDIA_TYPE_SLD_11,
    STYLE_FORMAT_TO_MEDIA_TYPE,
    f_param_to_media_type,
    normalize_accept_to_media_type,
)


def test_media_type_constants_are_exact():
    assert MEDIA_TYPE_SLD_11 == "application/vnd.ogc.sld+xml;version=1.1"
    assert MEDIA_TYPE_SLD_10 == "application/vnd.ogc.sld+xml"
    assert MEDIA_TYPE_MAPBOX_GL == "application/json"
    assert MEDIA_TYPE_QML == "application/xml"
    assert MEDIA_TYPE_FLAT_STYLE == "application/vnd.ogc.flat-style+json"
    assert MEDIA_TYPE_HTML == "text/html"


def test_style_format_to_media_type_mapping():
    # Matches existing StyleFormatEnum values in modules/styles/models.py.
    assert STYLE_FORMAT_TO_MEDIA_TYPE["MapboxGL"] == MEDIA_TYPE_MAPBOX_GL
    assert STYLE_FORMAT_TO_MEDIA_TYPE["SLD_1.1"] == MEDIA_TYPE_SLD_11


def test_f_param_shortcuts():
    assert f_param_to_media_type("mapbox") == MEDIA_TYPE_MAPBOX_GL
    assert f_param_to_media_type("sld11") == MEDIA_TYPE_SLD_11
    assert f_param_to_media_type("sld10") == MEDIA_TYPE_SLD_10
    assert f_param_to_media_type("qml") == MEDIA_TYPE_QML
    assert f_param_to_media_type("flat") == MEDIA_TYPE_FLAT_STYLE
    assert f_param_to_media_type("html") == MEDIA_TYPE_HTML
    assert f_param_to_media_type("json") == MEDIA_TYPE_MAPBOX_GL  # alias
    assert f_param_to_media_type("unknown") is None
    assert f_param_to_media_type(None) is None
    # case-insensitive
    assert f_param_to_media_type("MAPBOX") == MEDIA_TYPE_MAPBOX_GL


def test_accept_empty_picks_first_available():
    available = [MEDIA_TYPE_SLD_11, MEDIA_TYPE_MAPBOX_GL]
    assert normalize_accept_to_media_type("", available) == MEDIA_TYPE_SLD_11
    assert normalize_accept_to_media_type("*/*", available) == MEDIA_TYPE_SLD_11


def test_accept_no_available_returns_none():
    assert normalize_accept_to_media_type("application/json", []) is None


def test_accept_exact_match_with_version_param():
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    picked = normalize_accept_to_media_type(
        "application/vnd.ogc.sld+xml;version=1.1", available,
    )
    assert picked == MEDIA_TYPE_SLD_11


def test_accept_base_type_match_when_param_missing():
    # Caller asks for "application/vnd.ogc.sld+xml"; we have the 1.1 variant.
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    picked = normalize_accept_to_media_type("application/vnd.ogc.sld+xml", available)
    assert picked == MEDIA_TYPE_SLD_11


def test_accept_q_value_preference():
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    # SLD explicitly q=0.5 < JSON q=1.0 — JSON wins.
    picked = normalize_accept_to_media_type(
        "application/vnd.ogc.sld+xml;version=1.1;q=0.5, application/json", available,
    )
    assert picked == MEDIA_TYPE_MAPBOX_GL


def test_accept_unsupported_returns_none():
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    assert normalize_accept_to_media_type("image/png", available) is None


def test_accept_first_match_wins_within_equal_q():
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    # Two candidates with q=1 each; Accept order (JSON first) wins.
    picked = normalize_accept_to_media_type(
        "application/json, application/vnd.ogc.sld+xml;version=1.1", available,
    )
    assert picked == MEDIA_TYPE_MAPBOX_GL
