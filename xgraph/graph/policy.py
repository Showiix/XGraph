"""The traversal rules: how deep, what expands, what only gets recorded."""

from dataclasses import dataclass

from xgraph.domain import UserProfile

#: L5 is the business edge of the crawl. L5 accounts are still expanded, because
#: that request is what produces the L6 boundary; L6 accounts are recorded and
#: never expanded.
MAX_EXPAND_DEPTH = 5
BOUNDARY_DEPTH = MAX_EXPAND_DEPTH + 1


@dataclass(frozen=True, slots=True)
class TraversalPolicy:
    """Decides which discovered accounts get a request spent on them.

    The follower threshold is a traversal budget control, not a candidate
    decision. Filtered accounts stay in the graph, keep their profile and appear
    in the metrics; they simply do not consume a request of their own. Candidate
    selection happens on top of the finished graph and is a different question.
    """

    max_depth: int = MAX_EXPAND_DEPTH
    min_followers_to_expand: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.max_depth <= MAX_EXPAND_DEPTH:
            raise ValueError(f"max_depth must be between 0 and {MAX_EXPAND_DEPTH}")
        if self.min_followers_to_expand < 0:
            raise ValueError("min_followers_to_expand cannot be negative")

    @property
    def boundary_depth(self) -> int:
        """The depth at which accounts are recorded but never expanded."""

        return self.max_depth + 1

    def is_boundary(self, depth: int) -> bool:
        return depth >= self.boundary_depth

    def may_expand(self, depth: int) -> bool:
        return depth <= self.max_depth

    def filter_reason(self, profile: UserProfile | None) -> str | None:
        """Why this account will not be expanded, or None if it will be.

        A missing follower count is not treated as zero: an account whose
        profile the platform withheld is a gap in the evidence, not a small
        account, and dropping it silently would bias the graph towards whatever
        the platform happens to expose.
        """

        if self.min_followers_to_expand == 0:
            return None
        if profile is None or profile.followers_count is None:
            return None
        if profile.followers_count < self.min_followers_to_expand:
            return "below_follower_threshold"
        return None
