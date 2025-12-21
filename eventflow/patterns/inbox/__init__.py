"""
Transactional Inbox Pattern
============================

Reliable event consumption with exactly-once processing semantics.

Quick Start:
    from eventflow.patterns.inbox import (
        InboxProcessor,
        SQLAlchemyInboxRepository,
        RedisStreamTransport,
    )

    processor = InboxProcessor(
        transport=RedisStreamTransport(redis, "events", "my-group"),
        session_factory=async_session,
        repository_class=SQLAlchemyInboxRepository,
        event_handler=MyHandler(),
    )
    await processor.start()
"""

# What users run
from .processor import InboxProcessor

# What users receive in handlers
from .entity import InboxEntry, InboxStatus

# What users implement
from .ports import EventHandler

# What users instantiate
from .adapters import (
    InMemoryInboxRepository,
    RedisStreamTransport,
    SQLAlchemyInboxRepository,
)

# DLQ handling
from .dlq import InboxDLQEvent

__all__ = [
    # Core
    "InboxProcessor",
    "InboxEntry",
    "InboxStatus",
    "EventHandler",
    # Adapters
    "SQLAlchemyInboxRepository",
    "InMemoryInboxRepository",
    "RedisStreamTransport",
    # DLQ
    "InboxDLQEvent",
]
