"""
Inbox consumer for reliable event processing.

Extracted and generified from rasa-mach inbox_consumer.py.
Implements the Transactional Inbox pattern for exactly-once event processing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventflow.core.events import BaseEvent
from eventflow.patterns.inbox.repository import EventInboxRepository


logger = logging.getLogger(__name__)


class InboxConsumer:
    """
    Transactional Inbox consumer for reliable event processing.

    Implements the Inbox pattern:
    1. Pull events from Redis Streams
    2. Store in database inbox (durability)
    3. Process through business handlers
    4. Handle retries and dead-letter events

    Extracted from production code in rasa-mach project.
    """

    BLOCK_MS = 1_000
    BATCH_SIZE = 10

    def __init__(
        self,
        redis_client: Redis,
        session_factory: async_sessionmaker[AsyncSession],
        stream_name: str,
        consumer_group: str,
        consumer_name_prefix: str,
        event_handlers: Any,  # Your business handlers object
    ) -> None:
        """
        Initialize InboxConsumer.

        Args:
            redis_client: Redis async client for streams
            session_factory: SQLAlchemy async session factory
            stream_name: Redis stream name to consume from
            consumer_group: Redis consumer group name
            consumer_name_prefix: Prefix for consumer instance names
            event_handlers: Object with handle_event(session, inbox) method
        """
        self._redis = redis_client
        self._session_factory = session_factory
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._consumer_name = f"{consumer_name_prefix}-{socket.gethostname()}-{os.getpid()}"
        self._handlers = event_handlers
        self._running = False

    async def start(self) -> None:
        """Start consuming events until stopped."""
        await self._ensure_consumer_group()
        self._running = True
        logger.info("EventFlow inbox consumer started as %s", self._consumer_name)

        while self._running:
            processed_due = await self._process_due_events()
            had_messages = await self._drain_stream_once()

            if not had_messages and processed_due == 0:
                await asyncio.sleep(1)

    async def stop(self) -> None:
        """Signal the consumer loop to stop."""
        self._running = False
        await self._redis.aclose()
        logger.info("EventFlow inbox consumer stopped")

    async def _ensure_consumer_group(self) -> None:
        """Create the consumer group if it does not exist."""
        try:
            await self._redis.xgroup_create(
                name=self._stream_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info("Created Redis consumer group %s", self._consumer_group)
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("Consumer group %s already exists", self._consumer_group)
            else:
                raise

    async def _drain_stream_once(self) -> bool:
        """Read at most one batch from Redis and persist to the inbox."""
        try:
            messages = await self._redis.xreadgroup(
                groupname=self._consumer_group,
                consumername=self._consumer_name,
                streams={self._stream_name: ">"},
                count=self.BATCH_SIZE,
                block=self.BLOCK_MS,
            )
        except ResponseError as exc:
            logger.error("Redis responded with an error: %s", exc)
            await asyncio.sleep(2)
            return False

        if not messages:
            return False

        for _, entries in messages:
            for stream_id, entry_data in entries:
                stored = await self._store_inbox_entry(stream_id, entry_data)
                if stored:
                    logger.debug("Queued event %s for processing", stream_id)

        return True

    async def _store_inbox_entry(self, stream_id: str, entry_data: dict[str, Any]) -> bool:
        """Parse and persist a stream entry into the inbox."""
        try:
            event = self._parse_event(stream_id, entry_data)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to parse stream entry %s: %s", stream_id, exc, exc_info=True)
            await self._ack(stream_id)
            return False

        session: AsyncSession
        async with self._session_factory() as session:
            repo = EventInboxRepository(session)
            try:
                inbox_row = await repo.insert_pending(event)
                if not inbox_row:
                    logger.debug("Duplicate event %s, acknowledging", event.event_id)
                    await session.rollback()
                    await self._ack(stream_id)
                    return False

                await session.commit()
                logger.info("Stored event %s (%s)", event.event_id, event.event_type)
                await self._ack(stream_id)
                return True
            except Exception as exc:  # pylint: disable=broad-except
                await session.rollback()
                logger.error(
                    "Failed to persist event %s: %s",
                    event.event_id,
                    exc,
                    exc_info=True,
                )
                # Do not ACK so Redis can redeliver later
                return False

    async def _process_due_events(self) -> int:
        """Process inbox rows ready for handling."""
        processed_count = 0

        while self._running:
            session: AsyncSession
            async with self._session_factory() as session:
                repo = EventInboxRepository(session)
                due_events = await repo.acquire_due_events(self.BATCH_SIZE)

                if not due_events:
                    return processed_count

                for inbox_row in due_events:
                    try:
                        await repo.mark_processing(inbox_row)
                        await self._handlers.handle_event(session, inbox_row)
                        await repo.mark_processed(inbox_row)
                        await session.commit()
                        processed_count += 1
                        logger.info(
                            "Processed event %s (%s) for aggregate %s",
                            inbox_row.event_id,
                            inbox_row.event_type,
                            inbox_row.aggregate_id,
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        await session.rollback()
                        failure_session: AsyncSession
                        async with self._session_factory() as failure_session:
                            failure_repo = EventInboxRepository(failure_session)
                            await failure_repo.record_failure(cast(UUID, inbox_row.id), exc)
                            await failure_session.commit()
                        logger.error(
                            "Error processing event %s: %s",
                            inbox_row.event_id,
                            exc,
                            exc_info=True,
                        )

                # Continue loop to pick up more due events (if any)

        return processed_count

    async def _ack(self, stream_id: str) -> None:
        """Acknowledge processing of the stream entry."""
        await self._redis.xack(self._stream_name, self._consumer_group, stream_id)

    def _parse_event(self, stream_id: str, entry_data: dict[str, Any]) -> BaseEvent:
        """
        Deserialize a Redis stream entry into a BaseEvent.

        Supports payloads that either include a nested JSON blob under the `data`
        key or events that are already flattened.
        """
        if "data" in entry_data:
            raw = json.loads(entry_data["data"])
        else:
            raw = {
                key: json.loads(value) if self._looks_like_json(value) else value
                for key, value in entry_data.items()
            }

        event_id = raw.get("event_id") or entry_data.get("event_id")
        event_type = raw.get("event_type") or entry_data.get("event_type")
        aggregate_id = raw.get("aggregate_id")
        occurred_on = raw.get("occurred_on")
        payload = raw.get("payload", {})

        if not event_id or not event_type or not aggregate_id or not occurred_on:
            raise ValueError(f"Incomplete event payload for stream entry {stream_id}")

        return BaseEvent(
            event_id=str(event_id),
            event_type=str(event_type),
            aggregate_id=self._to_uuid(aggregate_id),
            occurred_on=self._to_datetime(occurred_on),
            payload=payload if isinstance(payload, dict) else {},
            correlation_id=raw.get("correlation_id"),
            causation_id=raw.get("causation_id"),
            stream_id=stream_id,
        )

    @staticmethod
    def _to_uuid(value: Any) -> UUID:
        if isinstance(value, UUID):
            return value
        return UUID(str(value))

    @staticmethod
    def _to_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)

    @staticmethod
    def _looks_like_json(value: str) -> bool:
        if not isinstance(value, str):
            return False
        value = value.strip()
        return (value.startswith("{") and value.endswith("}")) or (
            value.startswith("[") and value.endswith("]")
        )
