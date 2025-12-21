"""
Inbox Entry domain entity.

Pure Python business logic with no ORM or I/O dependencies.
Implements the state machine for event processing lifecycle.

This is the "inside" of the hexagon - pure domain logic that
can be tested without any infrastructure dependencies.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID


class InboxStatus(str, Enum):
    """
    Valid states for inbox entries.

    Inherits from str for easy serialization and database storage.

    State Machine:
        PENDING → PROCESSING → PROCESSED
                      ↓
                   FAILED ←→ (retry)
                      ↓
                 DEAD_LETTER
    """

    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class InboxEntry:
    """
    Domain entity representing an event in the inbox.

    This is a pure domain object with no ORM or transport dependencies.
    State transitions are validated and timestamps managed here.

    The entity encapsulates the business rules for event processing:
    - State machine transitions (pending → processing → processed/failed)
    - Retry counting and dead-letter detection
    - Error message management

    Attributes:
        id: Unique identifier for this inbox entry
        event_id: Original event identifier (for deduplication)
        source_id: Transport-agnostic source identifier (e.g., message ID from
                   any broker - Redis Stream ID, Kafka offset, RabbitMQ delivery tag)
        event_type: Type/name of the event (e.g., "OrderCreated")
        aggregate_id: ID of the aggregate this event belongs to
        occurred_on: When the original event occurred
        payload: Event data as dictionary
        status: Current processing state
        retry_count: Number of processing attempts
        max_retries: Maximum retries before dead-lettering
        correlation_id: Groups related events in a business operation (horizontal tracing)
        causation_id: Links to the event that caused this one (vertical parent-child)
        received_at: When event was received in inbox
        processed_at: When processing completed successfully
        next_retry_at: Scheduled retry time (for failed events)
        error_message: Last error message (truncated to 1000 chars)
        last_error_at: When last error occurred
    """

    # Required fields (no defaults)
    id: UUID
    event_id: str
    source_id: str  # Transport-agnostic: could be Redis stream ID, Kafka offset, etc.
    event_type: str
    aggregate_id: UUID
    occurred_on: datetime
    payload: Dict[str, Any]

    # State fields (with defaults)
    status: InboxStatus = InboxStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3

    # Tracing fields (for distributed systems observability)
    correlation_id: Optional[str] = None  # Groups all events in one business flow
    causation_id: Optional[str] = None  # The event that caused this event

    # Timestamp fields
    received_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    processed_at: Optional[datetime] = None
    next_retry_at: Optional[datetime] = None
    error_message: Optional[str] = None
    last_error_at: Optional[datetime] = None

    def mark_processing(self) -> None:
        """
        Transition to processing state.

        Called when a worker acquires this event for processing.
        Clears any previous retry scheduling.
        """
        self.status = InboxStatus.PROCESSING
        self.processed_at = None
        self.next_retry_at = None

    def mark_processed(self) -> None:
        """
        Mark as successfully processed.

        Clears all error state and retry counters.
        This is the terminal success state.
        """
        self.status = InboxStatus.PROCESSED
        self.processed_at = datetime.now(tz=timezone.utc)
        self.next_retry_at = None
        self.error_message = None
        self.last_error_at = None
        self.retry_count = 0

    def record_failure(
        self,
        message: str,
        retry_at: Optional[datetime],
        new_retry_count: int,
    ) -> bool:
        """
        Record a processing failure.

        Determines whether to retry or dead-letter based on retry count.
        Error messages are truncated to 1000 characters to prevent
        database bloat from stack traces.

        Args:
            message: Error message/description
            retry_at: When to retry (None if dead-lettering)
            new_retry_count: Updated retry count

        Returns:
            True if this is a permanent failure (dead-lettered),
            False if it will be retried.
        """
        self.retry_count = new_retry_count
        self.error_message = message[:1000] if message else None
        self.last_error_at = datetime.now(tz=timezone.utc)
        self.next_retry_at = retry_at

        if self.max_retries > 0 and new_retry_count >= self.max_retries:
            self.status = InboxStatus.DEAD_LETTER
            return True  # Permanent failure
        else:
            self.status = InboxStatus.FAILED
            return False  # Will retry

    @property
    def is_dead_lettered(self) -> bool:
        """Check if entry is in dead letter state."""
        return self.status == InboxStatus.DEAD_LETTER

    @property
    def is_processable(self) -> bool:
        """Check if entry can be processed (pending or failed with retry due)."""
        if self.status == InboxStatus.PENDING:
            return True
        if self.status == InboxStatus.FAILED:
            if self.next_retry_at is None:
                return True
            return self.next_retry_at <= datetime.now(tz=timezone.utc)
        return False
