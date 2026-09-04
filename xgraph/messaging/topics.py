"""Topic and consumer-group names for the raw page stream."""

#: Every observed X Web page, kept for replay. Keyed by account id so one
#: account's pages stay ordered and can be replayed together.
RAW_TOPIC = "x.pages.raw"

#: Terminal failures. Nothing here is retried automatically; a human or a
#: dedicated replay run decides what to do with it.
DLQ_TOPIC = "x.pages.dlq"

#: The group that turns raw pages into graph rows.
PARSER_GROUP = "xgraph-parser-v1"

#: Archives raw pages to cold storage. Reads the same stream independently.
ARCHIVER_GROUP = "xgraph-archiver-v1"

#: Aggregates DLQ entries for alerting.
DLQ_OBSERVER_GROUP = "xgraph-dlq-observer-v1"


def reparse_group(schema_version: int, run: str) -> str:
    """Name a one-off group that replays history after a parser change.

    A fresh group id starts at the beginning of the retained stream without
    disturbing the live parser's offsets, which is what makes a parser fix
    cost minutes of replay instead of a full re-crawl.
    """

    if not run or "/" in run:
        return f"xgraph-reparse-v{schema_version}"
    return f"xgraph-reparse-v{schema_version}-{run}"
