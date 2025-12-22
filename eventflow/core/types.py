"""Common type definitions for EventFlow."""

from typing import Any


# Event-related types
EventID = str
EventType = str
EventData = dict[str, Any]
StreamID = str
MessageID = str

# Status types
EventStatus = str  # "pending", "processing", "processed", "failed", "dead_letter"
