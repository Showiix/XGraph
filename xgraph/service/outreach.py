"""Who to approach first, who is in the same circle, and who can introduce you.

Everything here reads one structure: which of the accounts whose following lists
were actually collected follow a given candidate. Call those the voters. A
candidate's voter set is the only social position this dataset can observe — the
platform never told us who follows them outside this task, and nothing here
pretends otherwise.

Two consequences shape the API. Affinity is measured against what chance would
produce rather than in absolute terms, because every signature is sparse (a
typical candidate is followed by about a tenth of the voters) and raw overlap
therefore looks flat whether or not two accounts share a circle. And the
approach order is computed over the filtered set, not globally: the best eight
accounts to contact among everyone are not the best eight among the ones you can
actually message.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol

#: Ceiling on candidates pulled into the greedy selection. The work is
#: proportional to picks x candidates, and a caller asking for an approach order
#: over a million accounts wants a filter, not a longer wait.
MAX_CANDIDATE_POOL = 5_000

#: Minimum shared voters before an affinity is reported at all. Below this the
#: question is not how surprising the overlap is but whether anything was
#: observed.
MIN_SHARED_VOTERS = 8


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class ApproachStep:
    """One pick in the approach order, with what it adds and what it repeats."""

    account_id: str
    username: str | None
    new_reach: int
    cumulative_reach: int
    coverage_share: float
    voters: int
    network_indegree: int
    followers_count: int | None
    can_dm: bool | None


@dataclass(frozen=True, slots=True)
class ApproachPlan:
    steps: list[ApproachStep]
    voters_total: int
    candidates_considered: int
    bounded: bool
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class AffinePeer:
    account_id: str
    username: str | None
    shared_voters: int
    expected_shared: float
    lift: float
    #: How many standard deviations the overlap sits above chance. Unlike the
    #: ratio this keeps rising with evidence, so it can order peers of different
    #: sizes against each other.
    excess_sigma: float
    followers_count: int | None
    can_dm: bool | None


class OutreachQueries:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool

    async def approach_plan(
        self, task_id: str, account_ids: list[str], *, picks: int = 20
    ) -> ApproachPlan:
        """Order accounts so each pick reaches people the earlier ones did not.

        Ranking by in-degree alone keeps choosing the same corner of the circle:
        the second name is largely followed by the people who already follow the
        first, so the list grows long before it grows broad. Greedy maximum
        coverage is the standard answer and is within a known factor of optimal,
        which is the right trade when the input is a sample of a social graph
        rather than an exact one.
        """

        warnings: list[str] = []
        pool = account_ids[:MAX_CANDIDATE_POOL]
        if len(account_ids) > MAX_CANDIDATE_POOL:
            warnings.append(
                f"{len(account_ids)} accounts matched; the order was computed over the "
                f"first {MAX_CANDIDATE_POOL}"
            )
        if not pool:
            return ApproachPlan([], 0, 0, False, warnings)

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT target_account_id AS candidate, source_account_id AS voter
                FROM follow_edges
                WHERE task_id = $1 AND target_account_id = ANY($2::text[]);
                """,
                task_id,
                pool,
            )
            voters_total = int(
                await connection.fetchval(
                    "SELECT count(DISTINCT source_account_id) FROM follow_edges WHERE task_id = $1",
                    task_id,
                )
                or 0
            )
            meta = {
                str(r["account_id"]): r
                for r in await connection.fetch(
                    """
                    SELECT n.account_id, p.username, p.followers_count, p.can_dm,
                           n.network_indegree
                    FROM account_nodes n
                    LEFT JOIN account_profiles p
                      ON p.task_id = n.task_id AND p.account_id = n.account_id
                    WHERE n.task_id = $1 AND n.account_id = ANY($2::text[]);
                    """,
                    task_id,
                    pool,
                )
            }

        signatures: dict[str, set[str]] = {}
        for row in rows:
            signatures.setdefault(str(row["candidate"]), set()).add(str(row["voter"]))
        if not signatures:
            warnings.append("none of these accounts is followed by anyone whose list was collected")
            return ApproachPlan([], voters_total, 0, False, warnings)

        steps: list[ApproachStep] = []
        covered: set[str] = set()
        remaining = dict(signatures)
        while remaining and len(steps) < picks:
            pick = max(remaining, key=lambda a: (len(remaining[a] - covered), len(remaining[a])))
            gain = len(remaining[pick] - covered)
            if gain == 0:
                # Everyone left repeats ground already held. Stopping is the
                # answer: a longer list would look like progress and add none.
                warnings.append(
                    f"stopped at {len(steps)}: every remaining account reaches only people "
                    "already reached"
                )
                break
            covered |= remaining.pop(pick)
            row = meta.get(pick)
            steps.append(
                ApproachStep(
                    account_id=pick,
                    username=row["username"] if row else None,
                    new_reach=gain,
                    cumulative_reach=len(covered),
                    coverage_share=round(len(covered) / voters_total, 4) if voters_total else 0.0,
                    voters=len(signatures[pick]),
                    network_indegree=int(row["network_indegree"]) if row else 0,
                    followers_count=row["followers_count"] if row else None,
                    can_dm=row["can_dm"] if row else None,
                )
            )
        return ApproachPlan(
            steps=steps,
            voters_total=voters_total,
            candidates_considered=len(signatures),
            bounded=len(account_ids) > MAX_CANDIDATE_POOL,
            warnings=warnings,
        )

    async def affinity(self, task_id: str, account_id: str, *, limit: int = 20) -> list[AffinePeer]:
        """Accounts followed by the same people more than chance would explain.

        Ranked by how far the overlap sits from chance in standard deviations,
        not by the ratio. The ratio saturates: it cannot exceed the voter total
        divided by this account's own followers, so any peer whose small audience
        happens to sit inside this one's hits that ceiling exactly and the top of
        the list fills with accounts nobody has much evidence about. The ratio is
        still reported — it is the readable number — but it does not decide the
        order.
        """

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                WITH voters AS (
                    SELECT source_account_id AS v FROM follow_edges
                    WHERE task_id = $1 AND target_account_id = $2
                ),
                scale AS (
                    SELECT count(DISTINCT source_account_id)::numeric AS total,
                           (SELECT count(*) FROM voters)::numeric AS mine
                    FROM follow_edges WHERE task_id = $1
                ),
                peer AS (
                    SELECT e.target_account_id AS account_id, count(*)::numeric AS shared
                    FROM follow_edges e
                    JOIN voters ON voters.v = e.source_account_id
                    WHERE e.task_id = $1 AND e.target_account_id <> $2
                    GROUP BY 1
                    HAVING count(*) >= $3
                )
                SELECT peer.account_id, peer.shared, p.username, p.followers_count, p.can_dm,
                       n.network_indegree,
                       n.network_indegree * scale.mine / scale.total AS expected,
                       (peer.shared - n.network_indegree * scale.mine / scale.total)
                         / NULLIF(sqrt(n.network_indegree
                                       * (scale.mine / scale.total)
                                       * (1 - scale.mine / scale.total)), 0) AS z
                FROM peer
                CROSS JOIN scale
                JOIN account_nodes n ON n.task_id = $1 AND n.account_id = peer.account_id
                LEFT JOIN account_profiles p
                  ON p.task_id = $1 AND p.account_id = peer.account_id
                WHERE n.network_indegree > 0 AND scale.mine > 0 AND scale.mine < scale.total
                ORDER BY z DESC NULLS LAST, peer.shared DESC
                LIMIT $4;
                """,
                task_id,
                account_id,
                MIN_SHARED_VOTERS,
                limit,
            )
        peers: list[AffinePeer] = []
        for row in rows:
            expected = float(row["expected"] or 0)
            peers.append(
                AffinePeer(
                    account_id=str(row["account_id"]),
                    username=row["username"],
                    shared_voters=int(row["shared"]),
                    expected_shared=round(expected, 2),
                    lift=round(int(row["shared"]) / expected, 2) if expected else 0.0,
                    excess_sigma=round(float(row["z"] or 0), 2),
                    followers_count=row["followers_count"],
                    can_dm=row["can_dm"],
                )
            )
        return peers

    async def introductions(
        self, task_id: str, account_id: str, *, limit: int = 100
    ) -> dict[str, Any]:
        """Accounts inside this task that follow the target and can be messaged.

        The follow is the evidence: it says this person already chose to listen to
        the target. Whether they would make the introduction is not something a
        graph can answer, so the rows are ordered by their own standing in the
        circle and left for a human to read.
        """

        async with self._pool.acquire() as connection:
            total = await connection.fetchval(
                """
                SELECT count(*) FROM follow_edges e
                JOIN account_profiles p ON p.task_id = e.task_id
                                       AND p.account_id = e.source_account_id
                WHERE e.task_id = $1 AND e.target_account_id = $2
                  AND p.can_dm AND NOT COALESCE(p.protected, false);
                """,
                task_id,
                account_id,
            )
            rows = await connection.fetch(
                """
                SELECT p.account_id, p.username, p.followers_count, n.network_indegree
                FROM follow_edges e
                JOIN account_profiles p ON p.task_id = e.task_id
                                       AND p.account_id = e.source_account_id
                JOIN account_nodes n ON n.task_id = e.task_id
                                    AND n.account_id = e.source_account_id
                WHERE e.task_id = $1 AND e.target_account_id = $2
                  AND p.can_dm AND NOT COALESCE(p.protected, false)
                ORDER BY n.network_indegree DESC, p.followers_count DESC NULLS LAST
                LIMIT $3;
                """,
                task_id,
                account_id,
                limit,
            )
        return {
            "account_id": account_id,
            "total": int(total or 0),
            "limit": limit,
            "introducers": [dict(r) for r in rows],
        }
