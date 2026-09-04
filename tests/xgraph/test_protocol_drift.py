"""Guard against silent protocol drift between XGraph and the upstream snapshot.

XGraph deliberately does not import `twscrape` at runtime, so it owns its copy
of the GraphQL operation ids and feature flags. The consequence is that
`make update`, which rewrites only `twscrape/api.py`, cannot keep those copies
current: X rotates operation ids every few weeks and the copies would go stale
without any failing request until the next real crawl.

These tests are the missing link. They compare the two copies after every
`make update` run, so a rotation shows up as a failing test rather than as a
production outage.
"""

import twscrape.api as upstream
from xgraph.collector.operations import GQL_FEATURES, OPERATION_PATHS, USER_LOOKUP_FEATURES
from xgraph.domain import Operation

#: XGraph operation -> the upstream constant holding the same operation id.
UPSTREAM_OPERATION_CONSTANTS = {
    Operation.USER_BY_SCREEN_NAME: "OP_UserByScreenName",
    Operation.FOLLOWING: "OP_Following",
    Operation.USER_TWEETS: "OP_UserTweets",
}


def test_every_operation_is_mapped_to_an_upstream_constant() -> None:
    assert set(UPSTREAM_OPERATION_CONSTANTS) == set(Operation)
    assert set(OPERATION_PATHS) == set(Operation)


def test_operation_ids_match_the_upstream_snapshot() -> None:
    """X rotates GraphQL operation ids; a mismatch means the copy is stale.

    Fix by copying the upstream value into `xgraph/collector/operations.py`
    after `make update`, then re-running the fixture tests.
    """

    stale = {
        operation.value: (OPERATION_PATHS[operation], getattr(upstream, constant))
        for operation, constant in UPSTREAM_OPERATION_CONSTANTS.items()
        if OPERATION_PATHS[operation] != getattr(upstream, constant)
    }
    assert not stale, f"operation ids drifted from the upstream snapshot: {stale}"


def test_feature_flags_match_the_upstream_snapshot() -> None:
    """A missing feature flag makes X answer `(336) features cannot be null`."""

    ours = GQL_FEATURES
    theirs = upstream.GQL_FEATURES
    assert set(ours) - set(theirs) == set(), "XGraph declares flags upstream no longer sends"
    assert set(theirs) - set(ours) == set(), "upstream gained flags XGraph does not send"
    assert {key: ours[key] for key in ours if ours[key] != theirs[key]} == {}


def test_user_lookup_features_do_not_collide_with_the_shared_set() -> None:
    """The lookup overlay is merged on top of GQL_FEATURES at request time."""

    overridden = {
        key: (GQL_FEATURES[key], value)
        for key, value in USER_LOOKUP_FEATURES.items()
        if key in GQL_FEATURES and GQL_FEATURES[key] != value
    }
    assert not overridden, f"lookup overlay silently flips shared flags: {overridden}"
