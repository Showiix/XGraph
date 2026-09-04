-- XGraph phase 2 PostgreSQL schema. Credentials are referenced, not stored here.

CREATE TABLE IF NOT EXISTS crawl_tasks (
    task_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'created'
        CHECK (status IN ('created', 'validating', 'running', 'paused', 'completed', 'failed', 'terminated')),
    -- The open layer. Only frontier rows at this depth may be claimed, so a
    -- layer cannot start before the previous one has fully closed.
    current_depth INTEGER NOT NULL DEFAULT 0 CHECK (current_depth BETWEEN 0 AND 6),
    max_depth INTEGER NOT NULL DEFAULT 5 CHECK (max_depth BETWEEN 0 AND 5),
    -- Accounts below this follower count are recorded but never expanded. The
    -- filter is a traversal budget control, not a candidate decision: filtered
    -- nodes stay in the graph and in the metrics.
    min_followers_to_expand INTEGER NOT NULL DEFAULT 0 CHECK (min_followers_to_expand >= 0),
    -- Enrichment is optional and independently pausable. Turning it off must
    -- never affect the traversal, so nothing in the layer barrier reads it.
    timeline_enabled BOOLEAN NOT NULL DEFAULT true,
    max_requests BIGINT NOT NULL DEFAULT 100000,
    max_nodes BIGINT NOT NULL DEFAULT 1000000,
    max_edges BIGINT NOT NULL DEFAULT 5000000,
    requests_attempted BIGINT NOT NULL DEFAULT 0,
    nodes_created BIGINT NOT NULL DEFAULT 0,
    edges_created BIGINT NOT NULL DEFAULT 0,
    -- Completion is defined over three conditions, one of which is "the parser
    -- has caught up". Comparing these two counters answers that without asking
    -- the broker, and both sides update inside the same transaction as their
    -- business writes, so a redelivery cannot inflate either one.
    pages_produced BIGINT NOT NULL DEFAULT 0,
    pages_processed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS root_trees (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    tree_id TEXT NOT NULL,
    seed_account_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, tree_id),
    UNIQUE (task_id, seed_account_id)
);

CREATE TABLE IF NOT EXISTS account_nodes (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    -- Shortest depth at which the node was reached. Layers are processed in
    -- order, so the first durable insert already carries the shortest path and
    -- nothing later can improve on it.
    first_depth INTEGER NOT NULL CHECK (first_depth BETWEEN 0 AND 6),
    is_l6_boundary BOOLEAN NOT NULL DEFAULT false,
    expansion_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (expansion_status IN ('pending', 'queued', 'expanding', 'complete',
                                    'boundary', 'filtered', 'failed')),
    -- Coverage evidence. `declared` is what the platform says the account
    -- follows, `collected` is what it actually returned. Their ratio, together
    -- with the termination reason, is the only honest answer to "is this graph
    -- complete", and it must travel with the data rather than be inferred.
    declared_following INTEGER,
    collected_following INTEGER NOT NULL DEFAULT 0,
    -- Why the expansion chain stopped. Kept apart from the timeline's own
    -- outcome: both answer "why did a chain end", but they are different chains,
    -- and one column means the second writer silently destroys the first's
    -- answer -- which is what classifies this account's coverage.
    termination_reason TEXT,
    timeline_reason TEXT,
    -- How many accounts inside this task follow this one. Maintained with the
    -- edges rather than counted per query: it is the product's ranking signal,
    -- so every listing filters and sorts on it, and computing it per row turns
    -- a page of fifty into a scan of the whole task.
    network_indegree INTEGER NOT NULL DEFAULT 0,
    filter_reason TEXT,
    timeline_status TEXT NOT NULL DEFAULT 'none'
        CHECK (timeline_status IN ('none', 'candidate', 'queued', 'collecting',
                                   'complete', 'skipped', 'failed')),
    -- Which candidate conditions this account matched, kept as a list rather
    -- than a score: the PRD forbids dressing a heuristic up as a verdict, and a
    -- reviewer needs to see what actually matched.
    candidate_reasons TEXT[],
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, account_id)
);

CREATE INDEX IF NOT EXISTS account_nodes_depth_idx ON account_nodes (task_id, first_depth);

-- Profile fields as observed during this task. Kept beside the node rather than
-- inside it because a node exists as soon as it is referenced by an edge, while
-- its profile only arrives with the page that listed it.
CREATE TABLE IF NOT EXISTS account_profiles (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT,
    description TEXT,
    followers_count INTEGER,
    following_count INTEGER,
    created_at TIMESTAMPTZ,
    protected BOOLEAN,
    verified BOOLEAN,
    blue_verified BOOLEAN,
    can_dm BOOLEAN,
    location TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, account_id)
);

CREATE INDEX IF NOT EXISTS account_profiles_reachable_idx
    ON account_profiles (task_id, can_dm, followers_count);

CREATE TABLE IF NOT EXISTS account_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    tree_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    parent_account_id TEXT,
    depth INTEGER NOT NULL CHECK (depth BETWEEN 0 AND 6),
    is_collision BOOLEAN NOT NULL DEFAULT false,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (task_id, tree_id, account_id, depth, parent_account_id)
);

CREATE TABLE IF NOT EXISTS follow_edges (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    source_account_id TEXT NOT NULL,
    target_account_id TEXT NOT NULL,
    source_depth INTEGER NOT NULL CHECK (source_depth BETWEEN 0 AND 5),
    -- target_depth is the target node's shortest known depth, so an edge that
    -- points back at a Seed carries 0. Constraining it to 1..6 would reject
    -- exactly the closure edges that carry the strongest circle signal.
    target_depth INTEGER NOT NULL CHECK (target_depth BETWEEN 0 AND 6),
    is_l6_boundary BOOLEAN NOT NULL DEFAULT false,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, source_account_id, target_account_id)
);

CREATE TABLE IF NOT EXISTS follow_edge_observations (
    task_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    source_account_id TEXT NOT NULL,
    target_account_id TEXT NOT NULL,
    source_depth INTEGER NOT NULL CHECK (source_depth BETWEEN 0 AND 5),
    target_depth INTEGER NOT NULL CHECK (target_depth BETWEEN 0 AND 6),
    is_collision BOOLEAN NOT NULL DEFAULT false,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, tree_id, source_account_id, target_account_id),
    FOREIGN KEY (task_id, source_account_id, target_account_id)
        REFERENCES follow_edges(task_id, source_account_id, target_account_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crawl_frontier (
    frontier_id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    -- The tree that first queued this account. It travels with the page so the
    -- parser can attribute the discovery path; an account reached from several
    -- trees keeps one frontier row and several observations.
    tree_id TEXT,
    operation TEXT NOT NULL CHECK (operation IN ('Following', 'UserTweets', 'UserByScreenName')),
    depth INTEGER NOT NULL CHECK (depth BETWEEN 0 AND 5),
    cursor_in TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'retryable', 'failed', 'skipped')),
    priority INTEGER NOT NULL DEFAULT 0,
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    owner_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt INTEGER NOT NULL DEFAULT 0,
    -- Without a budget a poisoned row is reclaimed forever: every crash of its
    -- holder bumps `attempt` and the layer barrier in phase 4 can never close.
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    last_error_class TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (task_id, account_id, operation)
);

CREATE INDEX IF NOT EXISTS crawl_frontier_ready_idx
    ON crawl_frontier (status, not_before, priority DESC, depth, frontier_id);

CREATE TABLE IF NOT EXISTS task_operation_budgets (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    operation TEXT NOT NULL CHECK (operation IN ('Following', 'UserTweets', 'UserByScreenName')),
    max_requests BIGINT NOT NULL CHECK (max_requests > 0),
    requests_attempted BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, operation)
);

CREATE TABLE IF NOT EXISTS scraper_accounts (
    alias TEXT PRIMARY KEY,
    credential_ref TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    proxy_ref TEXT,
    tier TEXT NOT NULL DEFAULT 'primary' CHECK (tier IN ('primary', 'standby')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cooling', 'dead', 'standby')),
    error_class TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account_operation_quota (
    alias TEXT NOT NULL REFERENCES scraper_accounts(alias) ON DELETE CASCADE,
    operation TEXT NOT NULL CHECK (operation IN ('Following', 'UserTweets', 'UserByScreenName')),
    -- Seeded per operation by the account manager and overwritten by the first
    -- response headers; the column default is only a floor for manual inserts.
    limit_max INTEGER NOT NULL DEFAULT 150,
    remaining INTEGER NOT NULL DEFAULT 150,
    reset_at TIMESTAMPTZ,
    in_flight INTEGER NOT NULL DEFAULT 0 CHECK (in_flight >= 0),
    state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'cooling', 'leased', 'disabled')),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    consecutive_errors INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (alias, operation),
    -- Availability invariants. Each one forbids a state that the lease query
    -- can never select again, turning a silent pool leak into a loud failure.
    CONSTRAINT account_operation_quota_cooling_has_reset
        CHECK (state <> 'cooling' OR reset_at IS NOT NULL),
    CONSTRAINT account_operation_quota_leased_has_owner
        CHECK (state <> 'leased' OR lease_owner IS NOT NULL),
    CONSTRAINT account_operation_quota_ready_is_spendable
        CHECK (state <> 'ready' OR remaining > 0 OR reset_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS request_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    frontier_id BIGINT REFERENCES crawl_frontier(frontier_id),
    scraper_alias TEXT,
    operation TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT NOT NULL DEFAULT 'started'
        CHECK (outcome IN ('started', 'succeeded', 'rate_limited', 'failed')),
    status_code INTEGER,
    error_class TEXT
);

-- `event_id` identifies a page of a pagination chain and is stable across
-- retries, so re-fetching the same page cannot enqueue it twice. Scoping it by
-- task makes this the design's UNIQUE(task_id, account_id, operation, cursor_in).
CREATE TABLE IF NOT EXISTS raw_page_outbox (
    event_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    frontier_id BIGINT REFERENCES crawl_frontier(frontier_id),
    schema_version INTEGER NOT NULL DEFAULT 1,
    tree_id TEXT,
    account_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    depth INTEGER CHECK (depth IS NULL OR depth BETWEEN 0 AND 6),
    cursor_in TEXT,
    cursor_out TEXT,
    status_code INTEGER,
    requested_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    rate_limit JSONB,
    payload JSONB NOT NULL,
    -- Publish claim state. The publisher is at-least-once by construction: it
    -- may crash between `send` and `mark_published`, so a row can be sent more
    -- than once. Consumers absorb that through processed_events.
    publish_attempt INTEGER NOT NULL DEFAULT 0,
    publish_owner TEXT,
    publish_lease_expires_at TIMESTAMPTZ,
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, event_id)
);

CREATE INDEX IF NOT EXISTS raw_page_outbox_unpublished_idx
    ON raw_page_outbox (not_before, created_at, task_id, event_id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

-- The parser's idempotency ledger. Registering the event and performing the
-- business writes happen in one transaction, so a redelivered event either
-- collides here and is skipped, or is applied exactly once.
CREATE TABLE IF NOT EXISTS processed_events (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, task_id, event_id)
);

-- Consumer offsets live here rather than in the broker so that advancing the
-- offset and writing the graph commit or roll back together. `assignment_epoch`
-- fences a consumer that lost its partition in a rebalance but is still running.
CREATE TABLE IF NOT EXISTS consumer_offsets (
    group_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    committed_offset BIGINT NOT NULL DEFAULT -1,
    owner_id TEXT,
    assignment_epoch BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, topic, partition)
);

-- Terminal failures, kept queryable next to the graph. The broker topic carries
-- the same events for replay; this table is what alerting aggregates on.
CREATE TABLE IF NOT EXISTS dlq_events (
    dlq_id BIGSERIAL PRIMARY KEY,
    -- Deliberately not a foreign key. One of the failures this table exists to
    -- record is "the event's task no longer exists"; a reference to crawl_tasks
    -- would make exactly that case unrecordable, and would also delete the
    -- forensic trail at the moment somebody removes the task.
    task_id TEXT,
    group_id TEXT,
    event_id TEXT,
    topic TEXT NOT NULL,
    partition INTEGER,
    "offset" BIGINT,
    error_class TEXT NOT NULL,
    error_detail TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dlq_events_error_class_idx ON dlq_events (error_class, created_at DESC);

-- Qualifying-sample posts. Reposts and replies are stored too, marked as not
-- qualifying: they are evidence about the account's behaviour, and dropping
-- them would make the scan count unverifiable.
CREATE TABLE IF NOT EXISTS account_posts (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    post_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('original', 'quote', 'retweet', 'reply')),
    is_qualifying BOOLEAN NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    posted_at TIMESTAMPTZ,
    reply_count INTEGER NOT NULL DEFAULT 0,
    retweet_count INTEGER NOT NULL DEFAULT 0,
    like_count INTEGER NOT NULL DEFAULT 0,
    quote_count INTEGER NOT NULL DEFAULT 0,
    bookmark_count INTEGER NOT NULL DEFAULT 0,
    -- Nullable on purpose. The platform omits views on older posts, and a
    -- missing view count is not zero views; treating it as zero would drag
    -- every average towards nothing.
    view_count INTEGER,
    conversation_id TEXT,
    in_reply_to_tweet_id TEXT,
    quoted_tweet_id TEXT,
    retweeted_tweet_id TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, account_id, post_id)
);

CREATE INDEX IF NOT EXISTS account_posts_qualifying_idx
    ON account_posts (task_id, account_id, posted_at DESC)
    WHERE is_qualifying;

-- Derived from account_posts, never accumulated incrementally: recomputing from
-- the sample keeps the numbers traceable to the posts they came from, and makes
-- a redelivered page harmless.
CREATE TABLE IF NOT EXISTS account_metrics (
    task_id TEXT NOT NULL REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    scanned_count INTEGER NOT NULL DEFAULT 0,
    sample_span_days NUMERIC,
    latest_post_at TIMESTAMPTZ,
    avg_reply NUMERIC,
    avg_retweet NUMERIC,
    avg_like NUMERIC,
    -- Views are averaged over a different, smaller population than the other
    -- three, so its sample size is stored beside it. One number without the
    -- other cannot be interpreted.
    avg_view NUMERIC,
    view_sample_count INTEGER NOT NULL DEFAULT 0,
    median_engagement NUMERIC,
    engagement_rate NUMERIC,
    reach_ratio NUMERIC,
    bookmark_rate NUMERIC,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, account_id)
);

-- Additive migrations for databases created before these columns existed. The
-- CREATE TABLE statements above are all IF NOT EXISTS, so they are no-ops on an
-- existing database and cannot introduce a column on their own.
ALTER TABLE account_nodes ADD COLUMN IF NOT EXISTS timeline_reason TEXT;
ALTER TABLE account_nodes ADD COLUMN IF NOT EXISTS network_indegree INTEGER NOT NULL DEFAULT 0;

-- In-degree is looked up by target; the primary key leads with the source, so it
-- cannot serve that lookup. Without this index the product's ranking signal has
-- no index behind it at all.
CREATE INDEX IF NOT EXISTS follow_edges_target_idx
    ON follow_edges (task_id, target_account_id);

-- Listings order by in-degree within a task. The tiebreaker and the NULLS
-- ordering have to match the query's, or the planner sorts the whole task instead.
CREATE INDEX IF NOT EXISTS account_nodes_indegree_idx
    ON account_nodes (task_id, network_indegree DESC NULLS LAST, account_id);

-- Discovery paths are read per account. The unique constraint leads with
-- (task_id, tree_id), so it cannot serve that lookup, and every listed row falls
-- back to a sequential scan of every observation in the database.
CREATE INDEX IF NOT EXISTS account_observations_account_idx
    ON account_observations (task_id, account_id);

-- Same shape on the edge side: the primary key leads with the tree.
CREATE INDEX IF NOT EXISTS follow_edge_observations_edge_idx
    ON follow_edge_observations (task_id, source_account_id, target_account_id);
