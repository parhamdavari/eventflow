"""
Event Inbox repository.

Extracted and generified from rasa-mach workflow_event_inbox repository.
Handles database operations for the event inbox.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.core.events import BaseEvent
from eventflow.patterns.inbox.models import EventInbox


class EventInboxRepository:
    """Repository for event inbox operations."""

    def __init__(self, session: AsyncSession):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy async session
        """
        self._session = session

    async def insert_pending(self, event: BaseEvent) -> EventInbox | None:
        """
        Insert an event as pending in the inbox.

        Args:
            event: Event to insert

        Returns:
            EventInbox instance if inserted, None if event_id already exists
        """
        inbox = EventInbox(
            event_id=event.event_id,
            stream_id=event.stream_id or "",
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,  # type: ignore[arg-type]
            correlation_id=event.correlation_id,
            occurred_on=event.occurred_on,
            payload=event.payload,
        )
        self._session.add(inbox)

        try:
            await self._session.flush()
        except IntegrityError:
            await self._session.rollback()
            return None

        return inbox

    async def mark_processing(self, inbox: EventInbox) -> None:
        """
        Transition inbox row to processing state.

        Args:
            inbox: EventInbox instance
        """
        inbox.mark_processing()
        await self._session.flush()

    async def mark_processed(self, inbox: EventInbox) -> None:
        """
        Mark inbox row as successfully processed.

        Args:
            inbox: EventInbox instance
        """
        inbox.mark_processed()
        await self._session.flush()

    async def record_failure(
        self,
        inbox_id: UUID,
        error: Exception | str,
    ) -> EventInbox:
        """
        Record processing failure and schedule retry or dead-letter.

        Uses exponential backoff capped at 15 minutes.

        Args:
            inbox_id: Inbox row ID
            error: Error that occurred

        Returns:
            Updated EventInbox instance
        """
        stmt = select(EventInbox).where(EventInbox.id == inbox_id).with_for_update()
        result = await self._session.execute(stmt)
        inbox = result.scalar_one()

        current_retry_count = getattr(inbox, "retry_count", 0) or 0
        max_retries = getattr(inbox, "max_retries", 0) or 0

        new_retry_count = current_retry_count + 1
        dead_letter = bool(max_retries > 0 and new_retry_count >= max_retries)

        if dead_letter:
            retry_at = None
        else:
            # Exponential backoff capped at 15 minutes
            backoff_seconds = min(60 * (2 ** (new_retry_count - 1)), 15 * 60)
            retry_at = datetime.now(tz=timezone.utc) + timedelta(seconds=backoff_seconds)

        message = str(error)
        inbox.record_failure(
            message=message,
            retry_at=retry_at,
            retry_count=new_retry_count,
            dead_letter=dead_letter,
        )
        await self._session.flush()
        return inbox

    async def acquire_due_events(self, limit: int) -> list[EventInbox]:
        """
        Acquire events that are ready to be processed.

        Uses SELECT FOR UPDATE SKIP LOCKED for concurrent worker coordination.

        Args:
            limit: Maximum number of events to acquire

        Returns:
            List of EventInbox instances ready for processing
        """
        stmt = (
            select(EventInbox)
            .where(
                EventInbox.status.in_(["pending", "failed"]),
                or_(
                    EventInbox.next_retry_at.is_(None),
                    EventInbox.next_retry_at <= func.now(),
                ),
            )
            .order_by(EventInbox.received_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(stmt)
        return list(result.scalars().all())
