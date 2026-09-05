"""HTTP surface for the product.

Two rules shape everything here.

The API reads the same fact store the pipeline writes; there is no second
representation to drift. And it exposes only the graph — never the collection
machinery. Scraper aliases, credential references, proxies, quota state and
lease owners have no route, because a product surface that leaks them turns an
operational detail into an externally visible one.
"""

from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Annotated, Any, AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response

from xgraph.domain import CandidatePolicy
from xgraph.service.export import (
    ACCOUNT_COLUMNS,
    EDGE_COLUMNS,
    POST_COLUMNS,
    ExportService,
    to_csv,
    to_json,
)
from xgraph.service.outreach import MAX_CANDIDATE_POOL, OutreachQueries
from xgraph.service.queries import (
    MAX_PAGE_SIZE,
    AccountFilter,
    EdgeFilter,
    ProductQueries,
)
from xgraph.service.tasks import TaskService, TransitionError, parse_seed_list
from xgraph.storage.connection import apply_schema, create_pool
from xgraph.storage.timeline import PostgresTimelineStore


def create_app(dsn: str | None = None, pool: Any = None) -> FastAPI:
    """Build the application around an existing pool, or one it opens itself."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = pool is None
        app.state.pool = pool or await create_pool(str(dsn))
        if owned:
            await apply_schema(app.state.pool)
        try:
            yield
        finally:
            if owned:
                await app.state.pool.close()

    app = FastAPI(title="XGraph", version="0.1.0", lifespan=lifespan)

    def queries(request: Request) -> ProductQueries:
        return ProductQueries(request.app.state.pool)

    def tasks(request: Request) -> TaskService:
        return TaskService(request.app.state.pool)

    def outreach(request: Request) -> OutreachQueries:
        return OutreachQueries(request.app.state.pool)

    Queries = Annotated[ProductQueries, Depends(queries)]
    Tasks = Annotated[TaskService, Depends(tasks)]
    Outreach = Annotated[OutreachQueries, Depends(outreach)]

    # --- task control ---------------------------------------------------

    @app.post("/api/seeds/parse")
    async def parse_seeds(body: dict[str, Any]) -> dict[str, Any]:
        """Normalise a pasted list before anything is created.

        Reported separately rather than silently dropped: a typo that vanishes
        here becomes a missing tree that nobody notices later.
        """

        return parse_seed_list(str(body.get("text", ""))).as_dict

    @app.get("/api/tasks")
    async def list_tasks(service: Tasks) -> dict[str, Any]:
        return {"tasks": await service.list_tasks()}

    @app.get("/api/tasks/{task_id}")
    async def task_progress(task_id: str, service: Tasks) -> dict[str, Any]:
        return _found(await _guard(service.progress(task_id)))

    @app.post("/api/tasks/{task_id}/control")
    async def control(task_id: str, body: dict[str, Any], service: Tasks) -> dict[str, Any]:
        action = str(body.get("action", ""))
        target = {
            "start": "running",
            "pause": "paused",
            "resume": "running",
            "terminate": "terminated",
        }.get(action)
        if target is None:
            raise HTTPException(400, f"unknown action {action!r}")
        try:
            return await service.transition(task_id, target)
        except TransitionError as error:
            # A refused transition is information, not a server fault: the task
            # page shows why the button did nothing.
            raise HTTPException(409, str(error)) from error
        except LookupError as error:
            raise HTTPException(404, str(error)) from error

    @app.post("/api/tasks/{task_id}/timeline")
    async def toggle_timeline(
        task_id: str, body: dict[str, Any], request: Request
    ) -> dict[str, Any]:
        """Enrichment pauses independently; the traversal is unaffected."""

        store = PostgresTimelineStore(request.app.state.pool)
        await store.set_timeline_enabled(task_id, bool(body.get("enabled", True)))
        return await TaskService(request.app.state.pool).progress(task_id)

    @app.post("/api/tasks/{task_id}/candidates")
    async def admit_candidates(
        task_id: str, body: dict[str, Any], request: Request
    ) -> dict[str, Any]:
        policy = CandidatePolicy(
            require_can_dm=bool(body.get("require_can_dm", False)),
            exclude_protected=bool(body.get("exclude_protected", True)),
            min_followers=body.get("min_followers"),
            max_followers=body.get("max_followers"),
            min_network_indegree=body.get("min_network_indegree"),
            min_discovery_paths=body.get("min_discovery_paths"),
            bio_keywords=tuple(body.get("bio_keywords") or ()),
            max_depth=body.get("max_depth"),
            limit=int(body.get("limit", 500)),
        )
        store = PostgresTimelineStore(request.app.state.pool)
        admitted = await store.select_candidates(task_id, policy)
        return {"admitted": admitted, "conditions": list(policy.active_conditions)}

    # --- queries ---------------------------------------------------------

    @app.get("/api/tasks/{task_id}/accounts")
    async def accounts(
        task_id: str,
        service: Queries,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
        search: str | None = None,
        depth: Annotated[list[int] | None, Query()] = None,
        tree: Annotated[list[str] | None, Query()] = None,
        seeds_only: bool = False,
        boundary_only: bool = False,
        collisions_only: bool = False,
        can_dm: bool | None = None,
        verified: bool | None = None,
        protected: bool | None = None,
        min_followers: int | None = None,
        max_followers: int | None = None,
        min_network_indegree: int | None = None,
        has_timeline: bool | None = None,
        incomplete_only: bool = False,
        order_by: str = "network_indegree",
        descending: bool = True,
    ) -> dict[str, Any]:
        f = _account_filter(locals())
        page = await service.accounts(task_id, f)
        return _page(page, f.as_dict)

    @app.get("/api/tasks/{task_id}/accounts/{account_id}")
    async def account_detail(task_id: str, account_id: str, service: Queries) -> dict[str, Any]:
        detail = await service.account_detail(task_id, account_id)
        if detail is None:
            raise HTTPException(404, f"unknown account {account_id}")
        return detail

    @app.get("/api/tasks/{task_id}/outreach/plan")
    async def outreach_plan(
        task_id: str,
        service: Queries,
        outreach: Outreach,
        picks: Annotated[int, Query(ge=1, le=100)] = 20,
        pool_size: Annotated[int, Query(ge=1, le=MAX_CANDIDATE_POOL)] = 1000,
        search: str | None = None,
        depth: Annotated[list[int] | None, Query()] = None,
        tree: Annotated[list[str] | None, Query()] = None,
        seeds_only: bool = False,
        boundary_only: bool = False,
        collisions_only: bool = False,
        can_dm: bool | None = True,
        verified: bool | None = None,
        protected: bool | None = None,
        min_followers: int | None = None,
        max_followers: int | None = None,
        min_network_indegree: int | None = None,
        has_timeline: bool | None = None,
        incomplete_only: bool = False,
        order_by: str = "network_indegree",
        descending: bool = True,
    ) -> dict[str, Any]:
        """Order the matching accounts so each pick reaches people the others do not.

        The order is computed over the filtered set, not the whole task: the best
        accounts to contact among everyone are not the best among the ones you can
        actually message.
        """

        scope = dict(locals(), limit=1, offset=0)
        f = _account_filter(scope)
        ids = await service.account_ids(task_id, f, cap=pool_size)
        plan = await outreach.approach_plan(task_id, ids, picks=picks)
        return {
            "steps": [asdict(step) for step in plan.steps],
            "voters_total": plan.voters_total,
            "candidates_considered": plan.candidates_considered,
            "pool_size": len(ids),
            "bounded": plan.bounded or len(ids) >= pool_size,
            "warnings": plan.warnings,
            "filters": f.as_dict,
        }

    @app.get("/api/tasks/{task_id}/accounts/{account_id}/affinity")
    async def affinity(
        task_id: str,
        account_id: str,
        outreach: Outreach,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> dict[str, Any]:
        """Accounts followed by the same people far more often than chance would give."""

        peers = await outreach.affinity(task_id, account_id, limit=limit)
        return {"account_id": account_id, "peers": [asdict(peer) for peer in peers]}

    @app.get("/api/tasks/{task_id}/accounts/{account_id}/introductions")
    async def introductions(
        task_id: str,
        account_id: str,
        outreach: Outreach,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 100,
    ) -> dict[str, Any]:
        """Accounts in this task that follow the target and can be messaged."""

        return await outreach.introductions(task_id, account_id, limit=limit)

    @app.get("/api/tasks/{task_id}/relationships")
    async def relationships(
        task_id: str,
        service: Queries,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
        source: str | None = None,
        target: str | None = None,
        tree: Annotated[list[str] | None, Query()] = None,
        depth: Annotated[list[int] | None, Query()] = None,
        boundary_only: bool = False,
        collisions_only: bool = False,
    ) -> dict[str, Any]:
        f = EdgeFilter(
            source_id=source,
            target_id=target,
            tree_ids=tuple(tree or ()),
            depths=tuple(depth or ()),
            boundary_only=boundary_only,
            collisions_only=collisions_only,
            limit=limit,
            offset=offset,
        )
        return _page(await service.relationships(task_id, f), f.as_dict)

    @app.get("/api/tasks/{task_id}/subgraph")
    async def subgraph(
        task_id: str,
        service: Queries,
        seed: Annotated[list[str] | None, Query()] = None,
        hops: Annotated[int, Query(ge=1, le=3)] = 1,
        node_limit: Annotated[int, Query(ge=1)] = 200,
        min_network_indegree: int | None = None,
    ) -> dict[str, Any]:
        result = await service.subgraph(
            task_id,
            seeds=tuple(seed or ()),
            hops=hops,
            node_limit=node_limit,
            min_network_indegree=min_network_indegree,
        )
        return {
            "nodes": result.nodes,
            "edges": result.edges,
            "matched_nodes": result.matched_nodes,
            "node_limit": result.node_limit,
            "edge_limit": result.edge_limit,
            # The caller is told when it is seeing a slice, so a bounded answer
            # is never mistaken for the whole graph.
            "bounded": result.bounded,
            "warnings": result.warnings,
        }

    # --- exports ---------------------------------------------------------

    @app.get("/api/tasks/{task_id}/export/{dataset}")
    async def export(
        task_id: str,
        dataset: str,
        request: Request,
        fmt: Annotated[str, Query(pattern="^(csv|json)$")] = "csv",
        search: str | None = None,
        depth: Annotated[list[int] | None, Query()] = None,
        can_dm: bool | None = None,
        min_followers: int | None = None,
        min_network_indegree: int | None = None,
        incomplete_only: bool = False,
        source: str | None = None,
        target: str | None = None,
    ) -> Response:
        pool = request.app.state.pool
        service = ExportService(ProductQueries(pool), pool)
        if dataset == "accounts":
            rows, manifest = await service.accounts(
                task_id,
                AccountFilter(
                    search=search,
                    depths=tuple(depth or ()),
                    can_dm=can_dm,
                    min_followers=min_followers,
                    min_network_indegree=min_network_indegree,
                    incomplete_only=incomplete_only,
                ),
            )
            columns = ACCOUNT_COLUMNS
        elif dataset == "relationships":
            rows, manifest = await service.relationships(
                task_id, EdgeFilter(source_id=source, target_id=target)
            )
            columns = EDGE_COLUMNS
        elif dataset == "posts":
            rows, manifest = await service.posts(task_id)
            columns = POST_COLUMNS
        else:
            raise HTTPException(404, f"unknown dataset {dataset!r}")

        if fmt == "json":
            return Response(
                to_json(rows, manifest),
                media_type="application/json",
                headers=_export_headers(manifest.as_dict, task_id, dataset, "json"),
            )
        return Response(
            to_csv(rows, columns),
            media_type="text/csv",
            headers=_export_headers(manifest.as_dict, task_id, dataset, "csv"),
        )

    return app


def _export_headers(
    manifest: dict[str, Any], task_id: str, dataset: str, ext: str
) -> dict[str, str]:
    """Provenance travels with a CSV too, where the body has no room for it."""

    import json

    return {
        "content-disposition": f'attachment; filename="{task_id}-{dataset}.{ext}"',
        "x-xgraph-manifest": json.dumps(manifest, separators=(",", ":")),
    }


def _account_filter(scope: dict[str, Any]) -> AccountFilter:
    return AccountFilter(
        search=scope["search"],
        depths=tuple(scope["depth"] or ()),
        tree_ids=tuple(scope["tree"] or ()),
        seeds_only=scope["seeds_only"],
        boundary_only=scope["boundary_only"],
        collisions_only=scope["collisions_only"],
        can_dm=scope["can_dm"],
        verified=scope["verified"],
        protected=scope["protected"],
        min_followers=scope["min_followers"],
        max_followers=scope["max_followers"],
        min_network_indegree=scope["min_network_indegree"],
        has_timeline=scope["has_timeline"],
        incomplete_only=scope["incomplete_only"],
        order_by=scope["order_by"],
        descending=scope["descending"],
        limit=scope["limit"],
        offset=scope["offset"],
    )


def _page(page: Any, filters: dict[str, Any]) -> dict[str, Any]:
    return {
        "items": page.items,
        "total": page.total,
        "limit": page.limit,
        "offset": page.offset,
        "truncated": page.truncated,
        "filters": filters,
    }


async def _guard(coro: Any) -> Any:
    try:
        return await coro
    except LookupError as error:
        raise HTTPException(404, str(error)) from error


def _found(value: Any) -> Any:
    return value
