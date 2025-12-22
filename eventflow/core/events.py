"""
Core event models.

Provides domain-agnostic event abstractions that work with any transport or pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4


@dataclass(frozen=True)
class EventMetadata:
    """Metadata associated with an event."""

    event_id: str
    event_type: str
    aggregate_id: UUID
    occurred_on: datetime
    correlation_id: str | None = None
    causation_id: str | None = None
    stream_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary representation."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": str(self.aggregate_id),
            "occurred_on": self.occurred_on.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "stream_id": self.stream_id,
        }


@dataclass(frozen=True)
class BaseEvent:
    """
    Base event class for all domain events.

    This is the canonical representation of an event in the EventFlow system.
    Events are immutable facts about something that happened in the system.
    """

    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = ""
    aggregate_id: UUID = field(default_factory=uuid4)
    occurred_on: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None
    stream_id: str | None = None

    @property
    def metadata(self) -> EventMetadata:
        """Extract metadata from the event."""
        return EventMetadata(
            event_id=self.event_id,
            event_type=self.event_type,
            aggregate_id=self.aggregate_id,
            occurred_on=self.occurred_on,
            correlation_id=self.correlation_id,
            causation_id=self.causation_id,
            stream_id=self.stream_id,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary representation."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": str(self.aggregate_id),
            "occurred_on": self.occurred_on.isoformat(),
            "payload": self.payload,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "stream_id": self.stream_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BaseEvent:
        """
        Create event from dictionary representation.

        Args:
            data: Dictionary containing event data

        Returns:
            BaseEvent instance
        """
        occurred_on = data.get("occurred_on")
        if isinstance(occurred_on, str):
            occurred_on = datetime.fromisoformat(occurred_on.replace("Z", "+00:00"))

        aggregate_id = data.get("aggregate_id")
        if isinstance(aggregate_id, str):
            aggregate_id = UUID(aggregate_id)

        return cls(
            event_id=data.get("event_id", str(uuid4())),
            event_type=data.get("event_type", ""),
            aggregate_id=aggregate_id or uuid4(),
            occurred_on=occurred_on or datetime.now(tz=timezone.utc),
            payload=data.get("payload", {}),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            stream_id=data.get("stream_id"),
        )
