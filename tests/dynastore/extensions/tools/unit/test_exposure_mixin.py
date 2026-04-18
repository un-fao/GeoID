"""Tests for the service-exposure control panel mixin and registry."""

from pydantic import BaseModel
from dynastore.extensions.tools.exposure_mixin import (
    ExposableConfigMixin,
    KNOWN_EXTENSION_IDS,
    ALWAYS_ON_EXTENSIONS,
)


def test_mixin_defaults_enabled_true():
    class C(ExposableConfigMixin, BaseModel):
        pass
    assert C().enabled is True


def test_mixin_respects_explicit_false():
    class C(ExposableConfigMixin, BaseModel):
        pass
    assert C(enabled=False).enabled is False


def test_always_on_is_subset_of_known():
    assert ALWAYS_ON_EXTENSIONS.issubset(KNOWN_EXTENSION_IDS)


def test_always_on_includes_core_controlplane():
    for e in {"iam", "auth", "configs", "web", "admin", "documentation"}:
        assert e in ALWAYS_ON_EXTENSIONS
