#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

"""Notebooks extension preset — auto-register on import."""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


def _make_notebooks() -> object:
    from dynastore.extensions.notebooks.notebooks_extension import NotebooksExtension
    return NotebooksExtension.__new__(NotebooksExtension)


register_preset(PolicyContributorPreset(
    name="notebooks_enable",
    description="Notebooks extension IAM policies + anonymous read access to platform notebooks",
    keywords=("iam", "notebooks", "platform"),
    contributor_factory=_make_notebooks,
))
