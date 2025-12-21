"""
Dead Letter Queue (DLQ) observability components.

Provides shared metrics tracking and structured logging for dead-lettered events.
These components are pattern-agnostic and can be used by inbox, outbox, and saga.

When events permanently fail after max retries, they are:
1. Logged with [DLQ] prefix for alerting
2. Counted in metrics for monitoring
3. Optionally passed to callbacks for custom handling

Example:
    from eventflow.observability import DLQMetrics, log_dead_letter

    metrics = DLQMetrics()

    # When an event is dead-lettered
    log_dead_letter(
        event_id="evt-123",
        event_type="OrderCreated",
        aggregate_id=order_id,
        error_message="Database connection failed",
        retry_count=3,
        pattern="inbox",
    )
    await metrics.increment("OrderCreated")
"""

from __future__ import annotations

import logging
from asyncio import Lock as AsyncLock
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass
class DLQMetrics:
    """
    Async-safe, bounded metrics for dead-letter queue monitoring.

    Tracks total events and per-type counts with a configurable maximum
    number of unique event types to prevent unbounded memory growth.

    Thread-safe for concurrent async operations via asyncio.Lock.

    Example:
        metrics = DLQMetrics()
        await metrics.increment("OrderCreated")
        await metrics.increment("PaymentFailed")

        total = await metrics.get_total()  # 2
        by_type = await metrics.get_by_type()  # {"OrderCreated": 1, "PaymentFailed": 1}

    Attributes:
        _max_types: Maximum number of unique event types to track (default: 1000)
    """

    _events_total: int = 0
    _events_by_type: Dict[str, int] = field(default_factory=dict)
    _max_types: int = 1000
    _lock: AsyncLock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize the async lock after dataclass initialization."""
        object.__setattr__(self, "_lock", AsyncLock())

    async def increment(self, event_type: str) -> None:
        """
        Increment DLQ event counters.

        Thread-safe operation that updates both total and per-type counts.
        Per-type counts are bounded to prevent unbounded memory growth.

        Args:
            event_type: Type of event being dead-lettered
        """
        async with self._lock:
            self._events_total += 1
            if len(self._events_by_type) < self._max_types:
                self._events_by_type[event_type] = (
                    self._events_by_type.get(event_type, 0) + 1
                )
            elif event_type in self._events_by_type:
                # Allow incrementing existing types even at limit
                self._events_by_type[event_type] += 1
            else:
                logger.warning(
                    "DLQ metrics reached max event types (%d), ignoring new type: %s",
                    self._max_types,
                    event_type,
                )

    async def get_total(self) -> int:
        """
        Get total count of dead-lettered events.

        Returns:
            Total event count across all patterns and types
        """
        async with self._lock:
            return self._events_total

    async def get_by_type(self) -> Dict[str, int]:
        """
        Get event counts by type.

        Returns:
            Dictionary mapping event types to counts (copy to prevent mutation)
        """
        async with self._lock:
            return self._events_by_type.copy()

    def reset(self) -> None:
        """
        Reset all metrics.

        Useful for testing. Note: Not async-safe, only call during test setup.
        """
        self._events_total = 0
        self._events_by_type.clear()


def log_dead_letter(
    event_id: str,
    event_type: str,
    aggregate_id: UUID,
    error_message: str,
    retry_count: int,
    pattern: str = "inbox",
    correlation_id: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log dead letter event with [DLQ] prefix and structured fields.

    The [DLQ] prefix enables easy log aggregation and alerting rules.
    Structured extra fields support JSON logging and log aggregation systems
    like ELK, Datadog, or CloudWatch.

    Args:
        event_id: Unique event identifier
        event_type: Type of the event (e.g., "OrderCreated")
        aggregate_id: Aggregate the event belongs to
        error_message: Error that caused the permanent failure
        retry_count: Number of retry attempts before dead-lettering
        pattern: Event pattern that failed ("inbox", "outbox", "saga")
        correlation_id: Optional correlation ID for distributed tracing
        extra_fields: Optional additional fields for logging context

    Example:
        log_dead_letter(
            event_id="evt-123",
            event_type="PaymentProcessed",
            aggregate_id=order_id,
            error_message="Payment gateway timeout",
            retry_count=3,
            pattern="inbox",
            correlation_id="req-456",
        )
    """
    log_extra: Dict[str, Any] = {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_id": str(aggregate_id),
        "pattern": pattern,
        "correlation_id": correlation_id,
        "error_message": error_message,
        "retry_count": retry_count,
        "dead_letter_queue": True,
        "severity": "critical",
    }

    if extra_fields:
        log_extra.update(extra_fields)

    logger.error(
        "[DLQ] Event permanently failed after %d retries. "
        "pattern=%s, event_id=%s, event_type=%s, aggregate_id=%s, error=%s",
        retry_count,
        pattern,
        event_id,
        event_type,
        aggregate_id,
        error_message,
        extra=log_extra,
    )
