"""Outbox and message-stream boundaries for XGraph.

The pipeline separates the irreversible half of the system from the repeatable
one. A request to X spends quota and cannot be taken back; once its bytes are in
the outbox they can be re-interpreted any number of times for free. Everything
here exists to make that boundary hold across crashes on either side.
"""

from .broker import AssignmentHandler, Consumer, Message, Producer, TopicPartition
from .consumer import ConsumeResult, PageHandler, ParserRuntime, PermanentEventError
from .events import SCHEMA_VERSION, RawPageEvent
from .memory import InMemoryBroker, InMemoryConsumer, InMemoryProducer
from .publisher import OutboxPublisher, PublishResult
from .routing import RoutingPageHandler
from .topics import (
    ARCHIVER_GROUP,
    DLQ_OBSERVER_GROUP,
    DLQ_TOPIC,
    PARSER_GROUP,
    RAW_TOPIC,
    reparse_group,
)

__all__ = [
    "ARCHIVER_GROUP",
    "DLQ_OBSERVER_GROUP",
    "DLQ_TOPIC",
    "PARSER_GROUP",
    "RAW_TOPIC",
    "SCHEMA_VERSION",
    "AssignmentHandler",
    "ConsumeResult",
    "Consumer",
    "InMemoryBroker",
    "InMemoryConsumer",
    "InMemoryProducer",
    "Message",
    "OutboxPublisher",
    "PageHandler",
    "ParserRuntime",
    "PermanentEventError",
    "Producer",
    "PublishResult",
    "RawPageEvent",
    "RoutingPageHandler",
    "TopicPartition",
    "reparse_group",
]
