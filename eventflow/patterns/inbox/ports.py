"""
Ports (interfaces) for the Inbox pattern.

These protocols define the contracts that adapters must implement.
Following Hexagonal Architecture, the domain depends on these abstractions,
not concrete implementations.

Ports are organized by type:
- **Driven Ports** (Secondary): Called BY the application (Repository, Transport)
- **Driving Ports** (Primary): Call INTO the application (EventHandler)

Adapters implement these protocols:
- Repository: SQLAlchemy, MongoDB, In-Memory
- Transport: Redis Streams, Kafka, RabbitMQ
"""

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol
from uuid import UUID

from .entity import InboxEntry


class InboxRepository(Protocol):
    """
    Repository protocol for inbox persistence.

    This is a "driven port" (secondary port) - it's called BY the domain
    to persist and retrieve data. Adapters implement this interface.

    Implementations:
    - SQLAlchemyInboxRepository: Production PostgreSQL/SQLite
    - InMemoryInboxRepository: Fast unit testing
    - (future) MongoInboxRepository, DynamoDBInboxRepository, etc.
    """

    @abstractmethod
    async def insert_pending(self, entry: InboxEntry) -> Optional[InboxEntry]:
        """
        Insert a new entry in pending state.

        Used for idempotent event ingestion - if event_id already exists,
        returns None instead of raising an error.

        Args:
            entry: InboxEntry to insert

        Returns:
            The inserted entry, or None if event_id already exists
        """
        ...

    @abstractmethod
    async def get_by_id(self, entry_id: UUID) -> Optional[InboxEntry]:
        """
        Retrieve an entry by its unique ID.

        Args:
            entry_id: The inbox entry's UUID

        Returns:
            The entry if found, None otherwise
        """
        ...

    @abstractmethod
    async def save(self, entry: InboxEntry) -> None:
        """
        Persist changes to an existing entry.

        Called after domain operations modify the entry's state.
        Implementations should use optimistic or pessimistic locking
        to prevent lost updates.

        Args:
            entry: Modified InboxEntry to persist
        """
        ...

    @abstractmethod
    async def acquire_due_entries(self, limit: int) -> List[InboxEntry]:
        """
        Acquire entries that are ready for processing.

        This is the core method for worker coordination. Implementations
        should use appropriate locking (e.g., SELECT FOR UPDATE SKIP LOCKED)
        to ensure each entry is processed by exactly one worker.

        Returns entries that are:
        - PENDING status, or
        - FAILED status with next_retry_at <= now

        Args:
            limit: Maximum number of entries to acquire

        Returns:
            List of entries ready for processing
        """
        ...

    @abstractmethod
    async def list_dead_lettered(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: Optional[str] = None,
    ) -> List[InboxEntry]:
        """
        List dead-lettered entries for inspection or replay.

        Used by operators to view failed events and decide on recovery.

        Args:
            limit: Maximum number of entries to return
            offset: Number of entries to skip (for pagination)
            event_type: Optional filter by event type

        Returns:
            List of dead-lettered entries, ordered by last_error_at desc
        """
        ...

    @abstractmethod
    async def replay_dead_lettered(self, event_ids: List[str]) -> int:
        """
        Reset dead-lettered entries for replay.

        Called after fixing the root cause of failures. Resets entries
        to PENDING status with retry_count=0.

        Implementations should use locking to prevent concurrent replays.

        Args:
            event_ids: List of event IDs to replay

        Returns:
            Number of entries successfully reset
        """
        ...


class RetryPolicy(Protocol):
    """
    Protocol for retry delay calculation.

    Allows users to inject custom retry strategies (exponential backoff,
    linear backoff, fixed delay, etc.) without changing the consumer code.

    Example:
        class ExponentialBackoff:
            def calculate_next_retry(self, retry_count: int) -> datetime:
                delay = min(60 * (2 ** (retry_count - 1)), 900)  # Cap at 15 min
                return datetime.now(tz=timezone.utc) + timedelta(seconds=delay)
    """

    @abstractmethod
    def calculate_next_retry(self, retry_count: int) -> datetime:
        """
        Calculate when to retry based on attempt count.

        Args:
            retry_count: Number of failed attempts (1 = first failure)

        Returns:
            Datetime when the retry should be attempted
        """
        ...


class EventHandler(Protocol):
    """
    Protocol for event handling logic.

    This is a "driving port" (primary port) - it defines how the
    processor invokes business logic. Users implement this to handle
    their domain events.

    Example:
        class OrderEventHandler:
            async def handle(self, entry: InboxEntry, session: Any) -> None:
                if entry.event_type == "OrderCreated":
                    await self.on_order_created(entry.payload, session)
    """

    @abstractmethod
    async def handle(self, entry: InboxEntry, session: Any) -> None:
        """
        Handle an inbox entry.

        Called by the processor when an entry is ready for processing.
        If this method raises an exception, the entry will be marked
        as failed and retried according to the retry policy.

        Args:
            entry: The inbox entry to process
            session: Database session for transactional consistency
        """
        ...


# =============================================================================
# Transport Ports - Message consumption abstraction
# =============================================================================


@dataclass(frozen=True)
class RawMessage:
    """
    Transport-agnostic representation of a received message.

    Captures the minimal information needed from any message broker:
    - source_id: Unique identifier from the transport (for acknowledgment)
    - data: Raw message payload as dictionary

    This is a Value Object (immutable) that decouples the processor
    from transport-specific message formats.
    """

    source_id: str
    data: Dict[str, Any]


class MessageTransport(Protocol):
    """
    Protocol for message consumption from any transport.

    This is a "driven port" (secondary port) - called BY the processor
    to receive and acknowledge messages from external systems.

    Implementations:
    - RedisStreamTransport: Redis Streams with consumer groups
    - KafkaTransport: Apache Kafka (future)
    - RabbitMQTransport: RabbitMQ (future)
    - InMemoryTransport: Testing without real broker
    """

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the message broker.

        Called once at processor startup. Implementations should:
        - Establish network connections
        - Create consumer groups/subscriptions if needed
        - Handle reconnection logic internally
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Gracefully close connection to the message broker.

        Called at processor shutdown. Should release all resources.
        """
        ...

    @abstractmethod
    async def receive_batch(
        self,
        batch_size: int,
        timeout_ms: int,
    ) -> List[RawMessage]:
        """
        Receive a batch of messages from the transport.

        This is a blocking call that waits up to timeout_ms for messages.
        Returns an empty list if no messages are available.

        Args:
            batch_size: Maximum number of messages to receive
            timeout_ms: Maximum time to wait for messages

        Returns:
            List of RawMessage objects (may be empty)
        """
        ...

    @abstractmethod
    async def acknowledge(self, source_id: str) -> None:
        """
        Acknowledge successful processing of a message.

        Called after the message has been durably stored in the inbox.
        The transport should mark the message as processed and not
        redeliver it.

        Args:
            source_id: The source_id from the RawMessage to acknowledge
        """
        ...

    @property
    @abstractmethod
    def consumer_name(self) -> str:
        """
        Unique identifier for this consumer instance.

        Used for logging and monitoring. Typically includes
        hostname and process ID for distributed deployments.
        """
        ...
