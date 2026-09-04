"""Run the API: `python -m xgraph.api`.

The DSN comes from the environment rather than a flag so that a connection
string never lands in shell history or a process list.
"""

import os

import uvicorn

from .app import create_app


def main() -> None:
    dsn = os.getenv("XGRAPH_DATABASE_URL")
    if not dsn:
        raise SystemExit("XGRAPH_DATABASE_URL is required")
    uvicorn.run(
        create_app(dsn),
        host=os.getenv("XGRAPH_API_HOST", "127.0.0.1"),
        port=int(os.getenv("XGRAPH_API_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
