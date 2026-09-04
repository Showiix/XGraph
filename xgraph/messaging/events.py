"""The raw page event contract carried on `x.pages.raw`."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from xgraph.domain import Operation, PageEnvelope

#: Bumped when the event body changes shape. Consumers record it alongside the
#: event so a replay can tell which producer version wrote a given page.
SCHEMA_VERSION = 1


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_datetime(value: Any) -> datetime | None:
    return datetime.fromisoformat(value) if isinstance(value, str) and value else None


@dataclass(frozen=True, slots=True)
class RawPageEvent:
    """One observed page, carrying everything a re-parse needs.

    The parsed users and tweets are deliberately absent: the point of keeping
    the stream is that the payload can be re-interpreted by a newer parser, and
    shipping today's interpretation alongside it would freeze that decision.
    """

    event_id: str
    task_id: str
    account_id: str
    operation: Operation
    payload: dict[str, Any]
    schema_version: int = SCHEMA_VERSION
    tree_id: str | None = None
    depth: int | None = None
    cursor_in: str | None = None
    cursor_out: str | None = None
    status_code: int | None = None
    requested_at: datetime | None = None
    received_at: datetime | None = None
    rate_limit: dict[str, Any] | None = None

    @classmethod
    def from_envelope(
        cls,
        envelope: PageEnvelope,
        *,
        task_id: str,
        tree_id: str | None = None,
        depth: int | None = None,
    ) -> "RawPageEvent":
        return cls(
            event_id=envelope.event_id,
            task_id=task_id,
            account_id=envelope.source_account_id,
            operation=envelope.operation,
            payload=envelope.raw_payload,
            schema_version=SCHEMA_VERSION,
            tree_id=tree_id,
            depth=depth,
            cursor_in=envelope.cursor_in,
            cursor_out=envelope.cursor_out,
            status_code=envelope.status_code,
            requested_at=envelope.requested_at,
            received_at=envelope.received_at,
            rate_limit={
                "limit": envelope.rate_limit.limit,
                "remaining": envelope.rate_limit.remaining,
                "reset_at": _isoformat(envelope.rate_limit.reset_at),
            },
        )

    @property
    def key(self) -> str:
        """Partition key. One account's pages stay in order and replay together."""

        return self.account_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "tree_id": self.tree_id,
            "account_id": self.account_id,
            "operation": self.operation.value,
            "depth": self.depth,
            "cursor_in": self.cursor_in,
            "cursor_out": self.cursor_out,
            "status_code": self.status_code,
            "requested_at": _isoformat(self.requested_at),
            "received_at": _isoformat(self.received_at),
            "rate_limit": self.rate_limit,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RawPageEvent":
        return cls(
            event_id=str(data["event_id"]),
            task_id=str(data["task_id"]),
            account_id=str(data["account_id"]),
            operation=Operation(str(data["operation"])),
            payload=data["payload"],
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            tree_id=data.get("tree_id"),
            depth=data.get("depth"),
            cursor_in=data.get("cursor_in"),
            cursor_out=data.get("cursor_out"),
            status_code=data.get("status_code"),
            requested_at=_parse_datetime(data.get("requested_at")),
            received_at=_parse_datetime(data.get("received_at")),
            rate_limit=data.get("rate_limit"),
        )

    def serialize(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()

    @classmethod
    def deserialize(cls, raw: bytes) -> "RawPageEvent":
        return cls.from_dict(json.loads(raw.decode()))
