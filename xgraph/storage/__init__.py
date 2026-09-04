"""XGraph persistence boundary."""

from .connection import apply_schema, create_pool
from .events import (
    FencedConsumerError,
    OutboxRow,
    PipelineStats,
    PostgresEventStore,
    UnknownTaskError,
)
from .postgres import BudgetExhaustedError, FrontierItem, PostgresFrontierStore, RequestAttempt

__all__ = [
    "BudgetExhaustedError",
    "FencedConsumerError",
    "FrontierItem",
    "OutboxRow",
    "PipelineStats",
    "PostgresEventStore",
    "PostgresFrontierStore",
    "UnknownTaskError",
    "RequestAttempt",
    "apply_schema",
    "create_pool",
]
