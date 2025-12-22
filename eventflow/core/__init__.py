"""Core abstractions for EventFlow."""

from eventflow.core.events import BaseEvent, EventMetadata
from eventflow.core.protocols import EventHandler, Transport
from eventflow.core.types import EventData, EventID, EventType


__all__ = [
    "BaseEvent",
    "EventMetadata",
    "EventHandler",
    "Transport",
    "EventData",
    "EventID",
    "EventType",
]
