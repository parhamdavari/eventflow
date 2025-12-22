"""
EventFlow: Event-Driven Architecture Toolkit

A production-ready Python library for building reliable event-driven microservices
using Transactional Inbox/Outbox patterns.
"""

__version__ = "1.0.0"

from eventflow.core.events import BaseEvent, EventMetadata
from eventflow.patterns.inbox import InboxConsumer
from eventflow.patterns.outbox import OutboxPublisher
from eventflow.transports.redis_streams import RedisStreamsTransport


__all__ = [
    "BaseEvent",
    "EventMetadata",
    "InboxConsumer",
    "OutboxPublisher",
    "RedisStreamsTransport",
]
