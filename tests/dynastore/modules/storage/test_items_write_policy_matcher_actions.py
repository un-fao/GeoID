"""Unit tests for ItemsWritePolicy.matcher_actions (per-matcher conflict action)."""
from dynastore.modules.storage.driver_config import (
    IdentityMatcher,
    ItemsWritePolicy,
    WriteConflictPolicy,
)


def test_matcher_actions_default_is_none():
    """When unspecified, matcher_actions is None and global on_conflict is authoritative."""
    p = ItemsWritePolicy()
    assert p.matcher_actions is None


def test_matcher_actions_accepts_dict_of_matcher_to_action():
    """matcher_actions accepts a dict keyed by IdentityMatcher with WriteConflictPolicy values."""
    p = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        matcher_actions={
            IdentityMatcher.EXTERNAL_ID: WriteConflictPolicy.REFUSE_FAIL,
            IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN,
        },
    )
    assert p.matcher_actions is not None
    assert p.matcher_actions[IdentityMatcher.EXTERNAL_ID] == WriteConflictPolicy.REFUSE_FAIL
    assert p.matcher_actions[IdentityMatcher.GEOMETRY_HASH] == WriteConflictPolicy.REFUSE_RETURN


def test_matcher_actions_partial_override_is_allowed():
    """matcher_actions can specify only some matchers; others fall back to on_conflict."""
    p = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        on_conflict=WriteConflictPolicy.UPDATE,
        matcher_actions={IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN},
    )
    assert p.on_conflict == WriteConflictPolicy.UPDATE
    assert p.matcher_actions == {IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN}


def test_matcher_actions_serialises_to_string_keys_in_json():
    """JSON round-trip uses the StrEnum value of the matcher key."""
    p = ItemsWritePolicy(
        matcher_actions={IdentityMatcher.EXTERNAL_ID: WriteConflictPolicy.REFUSE_FAIL},
    )
    payload = p.model_dump(mode="json")
    assert payload["matcher_actions"] == {"external_id": "refuse_fail"}
