"""Candidate admission, post storage and metric derivation."""

from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from xgraph.domain import CandidatePolicy, Operation, TweetRecord


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class AccountMetrics:
    """Aggregates over an account's qualifying sample.

    `view_sample_count` is separate from `sample_count` because the platform
    omits views on older posts. Reporting an average view count without saying
    how many posts it came from would make a two-post average look like a
    thirty-post one.
    """

    account_id: str
    sample_count: int
    scanned_count: int
    sample_span_days: float | None
    latest_post_at: Any
    avg_reply: float | None
    avg_retweet: float | None
    avg_like: float | None
    avg_view: float | None
    view_sample_count: int
    median_engagement: float | None
    engagement_rate: float | None
    reach_ratio: float | None
    bookmark_rate: float | None


class PostgresTimelineStore:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool

    async def select_candidates(self, task_id: str, policy: CandidatePolicy) -> int:
        """Admit accounts and queue one timeline chain each.

        Admission and queueing happen in one statement so an account cannot be
        marked a candidate without the work item that follows from it.
        """

        async with self._pool.acquire() as connection, connection.transaction():
            enabled = await connection.fetchval(
                "SELECT timeline_enabled FROM crawl_tasks WHERE task_id = $1", task_id
            )
            if enabled is None:
                raise LookupError(f"unknown task {task_id}")
            if not enabled:
                return 0
            rows = await connection.fetch(
                """
                WITH indegree AS (
                    SELECT target_account_id AS account_id, count(*) AS n
                    FROM follow_edges WHERE task_id = $1
                    GROUP BY target_account_id
                ),
                paths AS (
                    SELECT account_id, count(DISTINCT tree_id) AS trees, count(*) AS n
                    FROM account_observations WHERE task_id = $1
                    GROUP BY account_id
                ),
                eligible AS (
                    SELECT n.account_id,
                           ARRAY_REMOVE(ARRAY[
                               CASE WHEN $2::bool AND p.can_dm THEN 'can_dm' END,
                               CASE WHEN $3::bool AND NOT COALESCE(p.protected, false)
                                    THEN 'not_protected' END,
                               CASE WHEN ($4::int IS NOT NULL OR $5::int IS NOT NULL)
                                    THEN 'follower_range' END,
                               CASE WHEN $6::int IS NOT NULL THEN 'network_indegree' END,
                               CASE WHEN $7::int IS NOT NULL THEN 'discovery_paths' END,
                               CASE WHEN cardinality($8::text[]) > 0 THEN 'bio_keyword' END,
                               CASE WHEN $9::int IS NOT NULL THEN 'depth' END
                           ], NULL) AS reasons
                    FROM account_nodes n
                    JOIN account_profiles p
                      ON p.task_id = n.task_id AND p.account_id = n.account_id
                    LEFT JOIN indegree d ON d.account_id = n.account_id
                    LEFT JOIN paths pa ON pa.account_id = n.account_id
                    WHERE n.task_id = $1
                      AND n.timeline_status = 'none'
                      AND (NOT $2::bool OR p.can_dm)
                      AND (NOT $3::bool OR NOT COALESCE(p.protected, false))
                      AND ($4::int IS NULL OR p.followers_count >= $4)
                      AND ($5::int IS NULL OR p.followers_count <= $5)
                      AND ($6::int IS NULL OR COALESCE(d.n, 0) >= $6)
                      AND ($7::int IS NULL OR COALESCE(pa.n, 0) >= $7)
                      AND ($9::int IS NULL OR n.first_depth <= $9)
                      AND (cardinality($8::text[]) = 0
                           OR EXISTS (SELECT 1 FROM unnest($8::text[]) AS kw
                                      WHERE p.description ILIKE '%' || kw || '%'))
                    ORDER BY COALESCE(d.n, 0) DESC, p.followers_count DESC NULLS LAST,
                             n.account_id
                    LIMIT $10
                ),
                queued AS (
                    INSERT INTO crawl_frontier(task_id, account_id, operation, depth)
                    SELECT $1, account_id, $11, 0 FROM eligible
                    ON CONFLICT (task_id, account_id, operation) DO NOTHING
                    RETURNING account_id
                )
                UPDATE account_nodes AS n
                SET timeline_status = 'queued',
                    candidate_reasons = e.reasons,
                    updated_at = now()
                FROM eligible AS e
                WHERE n.task_id = $1 AND n.account_id = e.account_id
                RETURNING n.account_id;
                """,
                task_id,
                policy.require_can_dm,
                policy.exclude_protected,
                policy.min_followers,
                policy.max_followers,
                policy.min_network_indegree,
                policy.min_discovery_paths,
                list(policy.bio_keywords),
                policy.max_depth,
                policy.limit,
                Operation.USER_TWEETS.value,
            )
        return len(rows)

    async def store_posts(
        self, connection: Any, *, task_id: str, account_id: str, posts: Sequence[TweetRecord]
    ) -> int:
        """Persist a page of posts, returning how many were new."""

        if not posts:
            return 0
        rows = await connection.fetch(
            """
            INSERT INTO account_posts(task_id, account_id, post_id, kind, is_qualifying,
                text, posted_at, reply_count, retweet_count, like_count, quote_count,
                bookmark_count, view_count, conversation_id, in_reply_to_tweet_id,
                quoted_tweet_id, retweeted_tweet_id)
            SELECT $1, $2, p.post_id, p.kind, p.is_qualifying, p.text, p.posted_at,
                   p.reply_count, p.retweet_count, p.like_count, p.quote_count,
                   p.bookmark_count, p.view_count, p.conversation_id,
                   p.in_reply_to_tweet_id, p.quoted_tweet_id, p.retweeted_tweet_id
            FROM unnest($3::text[], $4::text[], $5::bool[], $6::text[], $7::timestamptz[],
                        $8::int[], $9::int[], $10::int[], $11::int[], $12::int[], $13::int[],
                        $14::text[], $15::text[], $16::text[], $17::text[])
                 AS p(post_id, kind, is_qualifying, text, posted_at, reply_count,
                      retweet_count, like_count, quote_count, bookmark_count, view_count,
                      conversation_id, in_reply_to_tweet_id, quoted_tweet_id,
                      retweeted_tweet_id)
            ON CONFLICT (task_id, account_id, post_id) DO NOTHING
            RETURNING post_id;
            """,
            task_id,
            account_id,
            [p.id for p in posts],
            [p.kind.value for p in posts],
            [p.is_qualifying for p in posts],
            [p.text for p in posts],
            [p.created_at for p in posts],
            [p.reply_count for p in posts],
            [p.retweet_count for p in posts],
            [p.like_count for p in posts],
            [p.quote_count for p in posts],
            [p.bookmark_count for p in posts],
            [p.view_count for p in posts],
            [p.conversation_id for p in posts],
            [p.in_reply_to_tweet_id for p in posts],
            [p.quoted_tweet_id for p in posts],
            [p.retweeted_tweet_id for p in posts],
        )
        return len(rows)

    async def recompute_metrics(
        self, connection: Any, *, task_id: str, account_id: str, target_posts: int
    ) -> None:
        """Derive the aggregates from the stored sample.

        Recomputed rather than accumulated: the numbers stay traceable to the
        posts behind them, and a redelivered page cannot inflate them. The
        sample is capped at the newest `target_posts` qualifying entries, so a
        chain that overshot does not report more than the product promises.
        """

        await connection.execute(
            """
            WITH sample AS (
                SELECT * FROM account_posts
                WHERE task_id = $1 AND account_id = $2 AND is_qualifying
                ORDER BY posted_at DESC NULLS LAST
                LIMIT $3
            ),
            agg AS (
                SELECT
                    count(*) AS sample_count,
                    max(posted_at) AS latest_post_at,
                    EXTRACT(EPOCH FROM (max(posted_at) - min(posted_at))) / 86400
                        AS sample_span_days,
                    avg(reply_count) AS avg_reply,
                    avg(retweet_count) AS avg_retweet,
                    avg(like_count) AS avg_like,
                    -- Views average only over posts that reported one.
                    avg(view_count) FILTER (WHERE view_count IS NOT NULL) AS avg_view,
                    count(*) FILTER (WHERE view_count IS NOT NULL) AS view_sample_count,
                    percentile_cont(0.5) WITHIN GROUP (
                        ORDER BY reply_count + retweet_count + like_count
                    ) AS median_engagement,
                    -- Rates are computed only over posts that reported views,
                    -- so numerator and denominator cover the same population.
                    (sum(reply_count + retweet_count + like_count)
                        FILTER (WHERE view_count IS NOT NULL))::numeric
                        / NULLIF(sum(view_count) FILTER (WHERE view_count IS NOT NULL), 0)
                        AS engagement_rate,
                    (sum(bookmark_count) FILTER (WHERE view_count IS NOT NULL))::numeric
                        / NULLIF(sum(view_count) FILTER (WHERE view_count IS NOT NULL), 0)
                        AS bookmark_rate
                FROM sample
            )
            INSERT INTO account_metrics(task_id, account_id, sample_count, scanned_count,
                sample_span_days, latest_post_at, avg_reply, avg_retweet, avg_like,
                avg_view, view_sample_count, median_engagement, engagement_rate,
                reach_ratio, bookmark_rate, computed_at)
            SELECT $1, $2, agg.sample_count,
                   (SELECT count(*) FROM account_posts
                     WHERE task_id = $1 AND account_id = $2),
                   agg.sample_span_days, agg.latest_post_at, agg.avg_reply, agg.avg_retweet,
                   agg.avg_like, agg.avg_view, agg.view_sample_count, agg.median_engagement,
                   agg.engagement_rate,
                   agg.avg_view / NULLIF((SELECT followers_count FROM account_profiles
                                          WHERE task_id = $1 AND account_id = $2), 0),
                   agg.bookmark_rate, now()
            FROM agg
            ON CONFLICT (task_id, account_id) DO UPDATE SET
                sample_count = excluded.sample_count,
                scanned_count = excluded.scanned_count,
                sample_span_days = excluded.sample_span_days,
                latest_post_at = excluded.latest_post_at,
                avg_reply = excluded.avg_reply,
                avg_retweet = excluded.avg_retweet,
                avg_like = excluded.avg_like,
                avg_view = excluded.avg_view,
                view_sample_count = excluded.view_sample_count,
                median_engagement = excluded.median_engagement,
                engagement_rate = excluded.engagement_rate,
                reach_ratio = excluded.reach_ratio,
                bookmark_rate = excluded.bookmark_rate,
                computed_at = now();
            """,
            task_id,
            account_id,
            target_posts,
        )

    async def qualifying_progress(self, task_id: str, account_id: str) -> tuple[int, int]:
        """(qualifying, scanned) so far, used to decide whether to page again."""

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT count(*) FILTER (WHERE is_qualifying) AS qualifying,
                       count(*) AS scanned
                FROM account_posts WHERE task_id = $1 AND account_id = $2;
                """,
                task_id,
                account_id,
            )
        return int(row["qualifying"]), int(row["scanned"])

    async def set_timeline_status(
        self, task_id: str, account_id: str, status: str, *, reason: str | None = None
    ) -> None:
        if status not in {
            "none",
            "candidate",
            "queued",
            "collecting",
            "complete",
            "skipped",
            "failed",
        }:
            raise ValueError(f"invalid timeline status {status!r}")
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE account_nodes
                SET timeline_status = $3,
                    -- Its own column. Sharing `termination_reason` with the
                    -- expansion chain let a timeline outcome overwrite the fact
                    -- that classifies the account's follow coverage, and the two
                    -- writers never knew about each other.
                    timeline_reason = COALESCE($4, timeline_reason),
                    updated_at = now()
                WHERE task_id = $1 AND account_id = $2;
                """,
                task_id,
                account_id,
                status,
                reason,
            )

    async def set_timeline_enabled(self, task_id: str, enabled: bool) -> None:
        """Pause or resume enrichment without touching the traversal."""

        async with self._pool.acquire() as connection:
            await connection.execute(
                "UPDATE crawl_tasks SET timeline_enabled = $2, updated_at = now() "
                "WHERE task_id = $1",
                task_id,
                enabled,
            )

    async def metrics(self, task_id: str, account_id: str) -> AccountMetrics | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM account_metrics WHERE task_id = $1 AND account_id = $2",
                task_id,
                account_id,
            )
        if row is None:
            return None
        return AccountMetrics(
            account_id=account_id,
            sample_count=int(row["sample_count"]),
            scanned_count=int(row["scanned_count"]),
            sample_span_days=_float(row["sample_span_days"]),
            latest_post_at=row["latest_post_at"],
            avg_reply=_float(row["avg_reply"]),
            avg_retweet=_float(row["avg_retweet"]),
            avg_like=_float(row["avg_like"]),
            avg_view=_float(row["avg_view"]),
            view_sample_count=int(row["view_sample_count"]),
            median_engagement=_float(row["median_engagement"]),
            engagement_rate=_float(row["engagement_rate"]),
            reach_ratio=_float(row["reach_ratio"]),
            bookmark_rate=_float(row["bookmark_rate"]),
        )


def _float(value: Any) -> float | None:
    return float(value) if value is not None else None
