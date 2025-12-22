"""Common type definitions for EventFlow."""

from typing import Any, Dict

# Event-related types
EventID = str
EventType = str
EventData = Dict[str, Any]
StreamID = str
MessageID = str

# Status types
EventStatus = str  # "pending", "processing", "processed", "failed", "dead_letter"
