"""Contract pin for the liveness-reconciler PluginConfig fields on
``GcpModuleConfig`` — #735 plan step 12, deferred to #741.

#735 wired four runtime-tunable settings (Cloud Run spawn lease and the
reconciler's cadence / extension / grace windows) onto ``GcpModuleConfig`` as
``Mutable[...]`` fields so they can be reconfigured through the standard
PluginConfig surface without a redeploy. The plan's TDD step 12 ("PluginConfig
fields present + Mutable-marked; GcpJobRunner / reconciler read them") was not
landed with the original PR. This file closes that gap.

A regression here means either a field was renamed/removed (the runner /
reconciler will stop honouring operator overrides at runtime) or its
mutability marker was flipped (every config write would be rejected by the
immutability enforcer). Either is operationally invisible until the next time
an operator tries to tune the values — by which point ingestion is misbehaving.
"""

from __future__ import annotations

import inspect

import pytest

from dynastore.models.mutability import mutability_map


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — pure
    in-memory introspection."""
    return None


_LIVENESS_FIELDS = (
    "spawn_lease_seconds",
    "liveness_reconciler_interval_seconds",
    "liveness_extend_visibility_seconds",
    "liveness_unknown_grace_seconds",
)


def _gcp_module_config_cls():
    from dynastore.modules.gcp.gcp_config import GcpModuleConfig
    return GcpModuleConfig


# --- presence -------------------------------------------------------------


@pytest.mark.parametrize("field", _LIVENESS_FIELDS)
def test_liveness_field_is_declared_on_gcp_module_config(field):
    """Each of the four reconciler-tuning fields exists on ``GcpModuleConfig``."""
    assert field in _gcp_module_config_cls().model_fields, (
        f"Field {field!r} is gone from GcpModuleConfig — the GCP runner / "
        "liveness reconciler can no longer be tuned without a code change."
    )


# --- mutability marker ----------------------------------------------------


@pytest.mark.parametrize("field", _LIVENESS_FIELDS)
def test_liveness_field_is_marked_mutable(field):
    """Each field carries the ``Mutable`` marker — runtime-tunable on purpose.

    A non-``mutable`` marker (Immutable / WriteOnce / Computed) means the
    immutability enforcer rejects every operator-issued config update, which
    is the opposite of the intent ('configuration, not an environment variable')
    that drove #735.
    """
    kinds = mutability_map(_gcp_module_config_cls())
    assert kinds.get(field) == "mutable", (
        f"Field {field!r} mutability is {kinds.get(field)!r}; "
        "expected 'mutable' so operators can tune the value at runtime."
    )


# --- consumers actually read them -----------------------------------------


def test_gcp_runner_reads_spawn_lease_from_plugin_config():
    """The runner resolves the spawn lease via the config — not a bare env var.

    Source-inspection because the resolver is async and reaches into the
    config registry; the contract being pinned is that the field name and the
    config class are both referenced in the resolver.
    """
    from dynastore.modules.gcp import gcp_runner

    src = inspect.getsource(gcp_runner._resolve_spawn_lease_seconds)
    assert "GcpModuleConfig" in src
    assert "spawn_lease_seconds" in src


def test_gcp_module_lifespan_reads_reconciler_settings_from_plugin_config():
    """``GCPModule.lifespan`` constructs the reconciler with the three
    reconciler-side settings drawn from ``GcpModuleConfig`` — not from env."""
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module.GCPModule.lifespan)
    for field in (
        "liveness_reconciler_interval_seconds",
        "liveness_extend_visibility_seconds",
        "liveness_unknown_grace_seconds",
    ):
        assert field in src, (
            f"GCPModule.lifespan no longer reads {field!r} from PluginConfig — "
            "the reconciler will fall back to constructor defaults and ignore "
            "operator-issued config updates."
        )
