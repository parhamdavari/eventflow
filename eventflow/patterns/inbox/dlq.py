"""
Dead Letter Queue value object for the Inbox pattern.

Provides an immutable representation of dead-lettered inbox events
for callbacks, external publishing, and inspection.

Example:
    from eventflow.patterns.inbox import InboxDLQEvent, InboxEntry

    # From a dead-lettered entry
    dlq_event = InboxDLQEvent.from_entry(entry)
    print(f"Event {dlq_event.event_id} failed: {dlq_event.error_message}")

    # Serialize for external systems
    await send_to_alerting(dlq_event.to_dict())
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional
from uuid import UUID

if TYPE_CHECKING:
    from .entity import InboxEntry


@dataclass(frozen=True)
class InboxDLQEvent:
    """
    Immutable value object for dead-lettered inbox events.

    Wraps a failed inbox entry with DLQ-specific metadata for:
    - Alerting and monitoring callbacks
    - External system publishing (webhooks, streams)
    - Operational inspection and debugging

    Attributes:
        event_id: Unique identifier of the original event
        event_type: Type/name of the event (e.g., "OrderCreated")
        aggregate_id: Aggregate root ID the event belongs to
        payload: Original event payload
        occurred_on: When the original event occurred
        error_message: Final error that caused dead-lettering
        retry_count: Number of retry attempts before giving up
        dead_lettered_at: When the event was dead-lettered
        correlation_id: Groups related events in a business operation
        causation_id: Links to the event that caused this one
        source_id: Transport-agnostic source identifier
    """

    event_id: str
    event_type: str
    aggregate_id: UUID
    payload: Dict[str, Any]
    occurred_on: datetime
    error_message: str
    retry_count: int
    dead_lettered_at: datetime
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    source_id: Optional[str] = None

    @classmethod
    def from_entry(cls, entry: "InboxEntry") -> InboxDLQEvent:
        """
        Factory method to create from InboxEntry.

        Args:
            entry: Dead-lettered InboxEntry instance

        Returns:
            InboxDLQEvent instance with all metadata preserved

        Raises:
            ValueError: If entry is not in dead_letter status
        """
        if not entry.is_dead_lettered:
            raise ValueError(
                f"Cannot create DLQ event from non-dead-lettered entry "
                f"(status={entry.status})"
            )

        return cls(
            event_id=entry.event_id,
            event_type=entry.event_type,
            aggregate_id=entry.aggregate_id,
            payload=entry.payload,
            occurred_on=entry.occurred_on,
            error_message=entry.error_message or "",
            retry_count=entry.retry_count,
            dead_lettered_at=entry.last_error_at or datetime.now(tz=timezone.utc),
            correlation_id=entry.correlation_id,
            causation_id=entry.causation_id,
            source_id=entry.source_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to dictionary for external systems.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": str(self.aggregate_id),
            "payload": self.payload,
            "occurred_on": self.occurred_on.isoformat(),
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "dead_lettered_at": self.dead_lettered_at.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "source_id": self.source_id,
            "pattern": "inbox",
        }
