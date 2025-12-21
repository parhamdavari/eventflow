"""
Transport-agnostic Inbox processor.

Application service that orchestrates the Transactional Inbox pattern:
1. Receive messages from transport (Redis, Kafka, etc.)
2. Parse and store in database inbox (durability)
3. Process through business handlers (domain logic)
4. Handle retries and dead-letter events (reliability)

This is the "application layer" in Hexagonal Architecture - it coordinates
between driven ports (transport, repository) and driving ports (handlers).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Type
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .entity import InboxEntry, InboxStatus
from .ports import EventHandler, InboxRepository, MessageTransport, RawMessage, RetryPolicy

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """
    Default retry policy with exponential backoff.

    Delays: 1min → 2min → 4min → 8min → 15min (capped)
    """

    def __init__(self, base_seconds: int = 60, max_seconds: int = 900):
        self._base = base_seconds
        self._max = max_seconds

    def calculate_next_retry(self, retry_count: int) -> datetime:
        """Calculate next retry time with exponential backoff."""
        delay = min(self._base * (2 ** (retry_count - 1)), self._max)
        return datetime.now(tz=timezone.utc) + timedelta(seconds=delay)


class InboxProcessor:
    """
    Transport-agnostic processor for the Transactional Inbox pattern.

    Coordinates between:
    - MessageTransport: Receives messages from any broker
    - InboxRepository: Persists events for durability
    - EventHandler: Executes business logic
    - RetryPolicy: Manages failure recovery

    The processor runs two concurrent loops:
    1. Ingestion loop: Transport → Inbox (store for durability)
    2. Processing loop: Inbox → Handlers (execute business logic)

    This separation ensures events are never lost - once stored in the inbox,
    they will be processed even if the service restarts.
    """

    DEFAULT_BATCH_SIZE = 10
    DEFAULT_BLOCK_MS = 1_000

    def __init__(
        self,
        transport: MessageTransport,
        session_factory: async_sessionmaker[AsyncSession],
        repository_class: Type[InboxRepository],
        event_handler: EventHandler,
        retry_policy: Optional[RetryPolicy] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        block_ms: int = DEFAULT_BLOCK_MS,
        max_retries: int = 3,
    ) -> None:
        """
        Initialize the inbox processor.

        Args:
            transport: MessageTransport implementation (Redis, Kafka, etc.)
            session_factory: SQLAlchemy async session factory
            repository_class: InboxRepository implementation class
            event_handler: Business logic handler for events
            retry_policy: Custom retry strategy (default: ExponentialBackoff)
            batch_size: Messages to process per batch
            block_ms: Timeout for transport polling
            max_retries: Maximum retry attempts before dead-lettering
        """
        self._transport = transport
        self._session_factory = session_factory
        self._repository_class = repository_class
        self._handler = event_handler
        self._retry_policy = retry_policy or ExponentialBackoff()
        self._batch_size = batch_size
        self._block_ms = block_ms
        self._max_retries = max_retries
        self._running = False

    async def start(self) -> None:
        """
        Start the processor main loop.

        Connects to transport and continuously:
        1. Ingests new messages from transport to inbox
        2. Processes due entries from inbox through handlers
        """
        await self._transport.connect()
        self._running = True
        logger.info(
            "Inbox processor started as '%s'",
            self._transport.consumer_name,
        )

        while self._running:
            processed_due = await self._process_due_entries()
            had_messages = await self._ingest_messages()

            if not had_messages and processed_due == 0:
                await asyncio.sleep(1)

    async def stop(self) -> None:
        """Signal the processor to stop and disconnect transport."""
        self._running = False
        await self._transport.disconnect()
        logger.info("Inbox processor stopped")

    async def _ingest_messages(self) -> bool:
        """
        Pull messages from transport and store in inbox.

        Returns True if any messages were received.
        """
        messages = await self._transport.receive_batch(
            batch_size=self._batch_size,
            timeout_ms=self._block_ms,
        )

        if not messages:
            return False

        for message in messages:
            stored = await self._store_message(message)
            if stored:
                logger.debug(
                    "Ingested message %s to inbox",
                    message.source_id,
                )

        return True

    async def _store_message(self, message: RawMessage) -> bool:
        """
        Parse message and store as inbox entry.

        Returns True if stored, False if duplicate or parse error.
        """
        try:
            entry = self._parse_message(message)
        except Exception as exc:
            logger.error(
                "Failed to parse message %s: %s",
                message.source_id,
                exc,
                exc_info=True,
            )
            # ACK to prevent infinite redelivery of malformed messages
            await self._transport.acknowledge(message.source_id)
            return False

        async with self._session_factory() as session:
            repo = self._repository_class(session)
            try:
                result = await repo.insert_pending(entry)
                if result is None:
                    logger.debug(
                        "Duplicate event %s, acknowledging",
                        entry.event_id,
                    )
                    await session.rollback()
                    await self._transport.acknowledge(message.source_id)
                    return False

                await session.commit()
                logger.info(
                    "Stored event %s (%s)",
                    entry.event_id,
                    entry.event_type,
                )
                await self._transport.acknowledge(message.source_id)
                return True

            except Exception as exc:
                await session.rollback()
                logger.error(
                    "Failed to persist event %s: %s",
                    entry.event_id,
                    exc,
                    exc_info=True,
                )
                # Do NOT ACK - transport can redeliver
                return False

    async def _process_due_entries(self) -> int:
        """
        Process inbox entries that are ready for handling.

        Returns number of entries processed.
        """
        processed_count = 0

        while self._running:
            async with self._session_factory() as session:
                repo = self._repository_class(session)
                due_entries = await repo.acquire_due_entries(self._batch_size)

                if not due_entries:
                    return processed_count

                for entry in due_entries:
                    try:
                        entry.mark_processing()
                        await repo.save(entry)

                        await self._handler.handle(entry, session)

                        entry.mark_processed()
                        await repo.save(entry)
                        await session.commit()

                        processed_count += 1
                        logger.info(
                            "Processed event %s (%s) for aggregate %s",
                            entry.event_id,
                            entry.event_type,
                            entry.aggregate_id,
                        )

                    except Exception as exc:
                        await session.rollback()
                        await self._record_failure(entry.id, str(exc))
                        logger.error(
                            "Error processing event %s: %s",
                            entry.event_id,
                            exc,
                            exc_info=True,
                        )

        return processed_count

    async def _record_failure(self, entry_id: UUID, error_message: str) -> None:
        """Record failure with retry scheduling or dead-lettering."""
        async with self._session_factory() as session:
            repo = self._repository_class(session)
            entry = await repo.get_by_id(entry_id)

            if entry is None:
                logger.error("Entry %s not found for failure recording", entry_id)
                return

            new_retry_count = entry.retry_count + 1

            if self._max_retries > 0 and new_retry_count >= self._max_retries:
                retry_at = None  # Dead letter
            else:
                retry_at = self._retry_policy.calculate_next_retry(new_retry_count)

            is_dead_lettered = entry.record_failure(
                message=error_message,
                retry_at=retry_at,
                new_retry_count=new_retry_count,
            )

            await repo.save(entry)
            await session.commit()

            if is_dead_lettered:
                logger.warning(
                    "Event %s dead-lettered after %d retries: %s",
                    entry.event_id,
                    new_retry_count,
                    error_message[:100],
                )

    def _parse_message(self, message: RawMessage) -> InboxEntry:
        """
        Parse RawMessage into InboxEntry.

        Handles both nested JSON (data key) and flat structures.
        """
        data = message.data

        # Handle nested JSON blob
        if "data" in data and isinstance(data["data"], str):
            raw = json.loads(data["data"])
        else:
            raw = {
                key: json.loads(value) if self._looks_like_json(value) else value
                for key, value in data.items()
            }

        event_id = raw.get("event_id") or data.get("event_id")
        event_type = raw.get("event_type") or data.get("event_type")
        aggregate_id = raw.get("aggregate_id")
        occurred_on = raw.get("occurred_on")
        payload = raw.get("payload", {})

        if not event_id or not event_type or not aggregate_id or not occurred_on:
            raise ValueError(
                f"Incomplete event payload for message {message.source_id}"
            )

        return InboxEntry(
            id=uuid4(),
            event_id=str(event_id),
            source_id=message.source_id,
            event_type=str(event_type),
            aggregate_id=self._to_uuid(aggregate_id),
            occurred_on=self._to_datetime(occurred_on),
            payload=payload if isinstance(payload, dict) else {},
            correlation_id=raw.get("correlation_id"),
            causation_id=raw.get("causation_id"),
            max_retries=self._max_retries,
        )

    @staticmethod
    def _to_uuid(value: Any) -> UUID:
        """Convert value to UUID."""
        if isinstance(value, UUID):
            return value
        return UUID(str(value))

    @staticmethod
    def _to_datetime(value: Any) -> datetime:
        """Convert value to datetime."""
        if isinstance(value, datetime):
            return value
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)

    @staticmethod
    def _looks_like_json(value: Any) -> bool:
        """Check if string looks like JSON."""
        if not isinstance(value, str):
            return False
        value = value.strip()
        return (value.startswith("{") and value.endswith("}")) or (
            value.startswith("[") and value.endswith("]")
        )
