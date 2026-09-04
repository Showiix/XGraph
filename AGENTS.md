# XGraph repository guide

- `twscrape/` is an untouched upstream reference snapshot. Do not import it from XGraph runtime code or modify it for product behavior.
- `xgraph/` owns the independent protocol-level runtime: X Web GraphQL requests, Cookie sessions, signatures, Scraper Account pool, jobs, PostgreSQL data, BFS, profiling, API, messaging, and exports. There is no third-party API provider layer. Copy/adapt only required upstream logic, and record the source commit in `docs/upstream/BASELINE.md`.
- `web/` owns the browser application. Do not add frontend dependencies before implementing the first usable page.
- Preserve the upstream snapshot and its license. Keep X user and post IDs as strings across Python, PostgreSQL, JSON, and TypeScript.
- Run `make check` and `make test` for upstream compatibility fixtures, then run the focused XGraph tests before committing changes.
