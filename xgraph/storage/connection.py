"""PostgreSQL pool construction without introducing an ORM."""

from pathlib import Path
from typing import Any


async def create_pool(dsn: str, **kwargs: Any) -> Any:
    """Create an asyncpg pool; kept here so the rest of XGraph stays driver-neutral."""

    import asyncpg

    return await asyncpg.create_pool(dsn, **kwargs)


async def apply_schema(pool: Any) -> None:
    schema = Path(__file__).with_name("schema.sql").read_text()
    async with pool.acquire() as connection:
        await connection.execute(schema)
