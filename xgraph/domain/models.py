"""Platform data normalized for XGraph."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass(frozen=True, slots=True)
class UserProfile:
    """Public account fields observed in an X Web response."""

    id: str
    username: str
    display_name: str
    description: str
    followers_count: int | None
    following_count: int | None
    created_at: datetime | None
    protected: bool | None
    verified: bool | None
    blue_verified: bool | None
    can_dm: bool | None
    location: str | None
    avatar_url: str | None
    banner_url: str | None


class TweetKind(str, Enum):
    """Post type as defined by the product requirements.

    A single post can carry several platform flags at once, so the parser
    resolves exactly one kind with a fixed precedence:

        RETWEET > REPLY > QUOTE > ORIGINAL

    RETWEET wins because a retweet of a quote post still carries
    ``is_quote_status``, and the wrapper object holds no engagement of its own.
    REPLY wins over QUOTE because a reply that embeds a quote is still a reply,
    and the PRD excludes replies from the qualifying sample.
    """

    ORIGINAL = "original"
    QUOTE = "quote"
    RETWEET = "retweet"
    REPLY = "reply"


#: Kinds that may enter the 30-post qualifying sample. Pure reposts and replies
#: are excluded; the wrapper object of a repost reports zero engagement and
#: would otherwise halve every average.
QUALIFYING_TWEET_KINDS = frozenset({TweetKind.ORIGINAL, TweetKind.QUOTE})


@dataclass(frozen=True, slots=True)
class TweetRecord:
    """A normalized tweet record observed in an X Web timeline response."""

    id: str
    author_id: str | None
    text: str
    created_at: datetime | None
    kind: TweetKind
    reply_count: int
    retweet_count: int
    like_count: int
    quote_count: int
    bookmark_count: int
    view_count: int | None
    conversation_id: str | None
    in_reply_to_tweet_id: str | None
    in_reply_to_user_id: str | None
    retweeted_tweet_id: str | None
    quoted_tweet_id: str | None

    @property
    def is_qualifying(self) -> bool:
        """Whether this post may enter the qualifying sample."""

        return self.kind in QUALIFYING_TWEET_KINDS

    @property
    def is_self_thread_reply(self) -> bool:
        """A reply to the author's own post, i.e. a thread continuation."""

        return (
            self.kind is TweetKind.REPLY
            and self.in_reply_to_user_id is not None
            and self.in_reply_to_user_id == self.author_id
        )
