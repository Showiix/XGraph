"""Enrichment policy: who gets a timeline, and how much of it.

These are value objects describing how a task is configured, so they live in the
domain rather than beside either the storage or the scheduler that reads them.
Putting them next to one of those would make the other depend on it.

Which accounts are worth spending timeline requests on.

Admission runs on fields the traversal already collected for free, so deciding
costs nothing. That matters: a timeline chain costs roughly ten requests, about
the same as expanding an account, and expanding one account yields hundreds of
new ones. Enriching everything would cost more than building the graph.

Nothing here produces a score. The PRD is explicit that a heuristic must not be
presented as a verdict, so the output is the list of conditions an account
matched, which a reviewer can read and disagree with.
"""

from dataclasses import dataclass, field
from enum import Enum

#: The qualifying sample size the product asks for.
TARGET_QUALIFYING_POSTS = 30

#: How many timeline entries may be scanned to find them. Measured qualifying
#: rate on a real page was 4 in 21; taking the lower bound of that observation
#: puts 30 qualifying posts at roughly 200 scanned entries.
MAX_POSTS_SCANNED = 200


@dataclass(frozen=True, slots=True)
class CandidatePolicy:
    """Admission rules over already-collected fields.

    Every bound is optional. An unset bound is not a silent default: it means
    the dimension is not being used, and no account is rejected for it.
    """

    require_can_dm: bool = False
    exclude_protected: bool = True
    min_followers: int | None = None
    max_followers: int | None = None
    min_network_indegree: int | None = None
    min_discovery_paths: int | None = None
    bio_keywords: tuple[str, ...] = ()
    max_depth: int | None = None
    limit: int = 500

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be positive")
        if (
            self.min_followers is not None
            and self.max_followers is not None
            and self.min_followers > self.max_followers
        ):
            raise ValueError("min_followers cannot exceed max_followers")

    @property
    def active_conditions(self) -> tuple[str, ...]:
        """The dimensions this policy actually constrains."""

        names: list[str] = []
        if self.require_can_dm:
            names.append("can_dm")
        if self.exclude_protected:
            names.append("not_protected")
        if self.min_followers is not None or self.max_followers is not None:
            names.append("follower_range")
        if self.min_network_indegree is not None:
            names.append("network_indegree")
        if self.min_discovery_paths is not None:
            names.append("discovery_paths")
        if self.bio_keywords:
            names.append("bio_keyword")
        if self.max_depth is not None:
            names.append("depth")
        return tuple(names)


@dataclass(frozen=True, slots=True)
class TimelinePolicy:
    """How much of a timeline to collect once an account is admitted."""

    target_posts: int = TARGET_QUALIFYING_POSTS
    max_scanned: int = MAX_POSTS_SCANNED
    max_pages: int = 12
    max_post_age_days: int | None = None
    candidate: CandidatePolicy = field(default_factory=CandidatePolicy)

    def __post_init__(self) -> None:
        if self.target_posts < 1:
            raise ValueError("target_posts must be positive")
        if self.max_scanned < 1:
            raise ValueError("max_scanned must be positive")
        # `max_scanned` below `target_posts` is deliberate, not a mistake: it
        # caps the cost and accepts a smaller sample. Only about one entry in
        # five is a qualifying post, so an account that mostly reposts would
        # otherwise be paged far past what it is worth.
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive")

    def enough(self, qualifying: int, scanned: int) -> bool:
        """Whether the sample is finished, by either sufficiency or budget."""

        return qualifying >= self.target_posts or scanned >= self.max_scanned


class TerminationReason(str, Enum):
    """Why a pagination chain stopped. Recorded per account, not inferred later.

    Lives in the domain rather than the scheduler because storage and the query
    layer both have to tell one kind of stop from another, and a coverage ratio
    means different things on either side of that line.
    """

    NATURAL_END = "natural_end"
    EMPTY_PAGES = "empty_pages"
    CURSOR_STALLED = "cursor_stalled"
    PAGE_LIMIT = "page_limit"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FAILED = "failed"


#: Chains the platform ended. `collected / declared` is a statement about what X
#: was willing to hand over, so it is a coverage figure.
PLATFORM_TERMINATIONS = frozenset(
    {
        TerminationReason.NATURAL_END.value,
        TerminationReason.EMPTY_PAGES.value,
        TerminationReason.CURSOR_STALLED.value,
    }
)

#: Chains we ended. The same ratio here measures our own budget, not the
#: platform's completeness; averaging the two together makes both unreadable.
SELF_TERMINATIONS = frozenset(
    {
        TerminationReason.PAGE_LIMIT.value,
        TerminationReason.BUDGET_EXHAUSTED.value,
    }
)
