#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

"""Configs extension preset — auto-register on import."""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


def _make_configs() -> object:
    from dynastore.extensions.configs.service import ConfigsService
    return ConfigsService.__new__(ConfigsService)


register_preset(PolicyContributorPreset(
    name="configs_enable",
    description="Configuration management extension IAM policies (sysadmin-tier access)",
    keywords=("iam", "configs", "platform"),
    contributor_factory=_make_configs,
))
