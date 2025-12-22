"""
Protocol definitions for EventFlow.

Defines interfaces that can be implemented by different transports and handlers.
Uses Python's Protocol for structural subtyping (duck typing with type checking).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable

from eventflow.core.events import BaseEvent


@runtime_checkable
class EventHandler(Protocol):
    """
    Protocol for event handlers.

    Handlers implement business logic that reacts to events.
    """

    async def handle(self, event: BaseEvent) -> None:
        """
        Handle an event.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails
        """
        ...


@runtime_checkable
class Transport(Protocol):
    """
    Protocol for event transports.

    Transports are responsible for publishing and consuming events from
    a messaging system (Redis Streams, Kafka, SQS, etc.).
    """

    async def publish(self, stream_name: str, event: BaseEvent) -> str:
        """
        Publish an event to a stream.

        Args:
            stream_name: Name of the stream to publish to
            event: Event to publish

        Returns:
            Message ID or identifier from the transport

        Raises:
            TransportError: If publishing fails
        """
        ...

    async def consume(
        self, stream_name: str, consumer_group: str, consumer_name: str, count: int = 10
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """
        Consume events from a stream.

        Args:
            stream_name: Name of the stream to consume from
            consumer_group: Consumer group name
            consumer_name: Consumer instance name
            count: Number of messages to fetch

        Yields:
            Tuple of (message_id, event_data)

        Raises:
            TransportError: If consuming fails
        """
        ...

    async def acknowledge(self, stream_name: str, consumer_group: str, message_id: str) -> None:
        """
        Acknowledge processing of a message.

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            message_id: Message ID to acknowledge

        Raises:
            TransportError: If acknowledgment fails
        """
        ...

    async def close(self) -> None:
        """Close the transport connection."""
        ...
