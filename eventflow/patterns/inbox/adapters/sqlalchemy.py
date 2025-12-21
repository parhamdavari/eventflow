"""
SQLAlchemy adapter for Inbox persistence.

Implements the InboxRepository protocol using SQLAlchemy ORM.
This adapter provides PostgreSQL-optimized storage with JSONB support,
falling back to JSON for SQLite in testing environments.

This is the "outside" of the hexagon - it knows about specific database
technologies but isolates that knowledge from the domain layer.
"""

from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, declarative_mixin
from sqlalchemy.types import JSON, TypeDecorator

from ..entity import InboxEntry, InboxStatus


class JSONBCompat(TypeDecorator):
    """
    Cross-database JSON column type.

    Uses PostgreSQL JSONB for production (with indexing and operators),
    falls back to generic JSON for SQLite in unit tests.
    """

    impl = JSONB
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB(astext_type=Text()))
        return dialect.type_descriptor(JSON())


class Base(DeclarativeBase):
    """Default declarative base for standalone EventFlow usage."""

    pass


@declarative_mixin
class EventInboxMixin:
    """
    SQLAlchemy mixin for inbox table definition.

    Use this mixin with your application's own declarative base
    when you need to integrate with existing database models.

    Example:
        from eventflow.patterns.inbox.adapters.sqlalchemy import EventInboxMixin
        from my_app.database import Base

        class MyEventInbox(EventInboxMixin, Base):
            __tablename__ = "my_custom_inbox"
    """

    __tablename__ = "event_inbox"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id = Column(String(255), nullable=False, unique=True)
    source_id = Column(String(255), nullable=False)  # Transport-agnostic identifier
    event_type = Column(String(128), nullable=False)
    aggregate_id = Column(PGUUID(as_uuid=True), nullable=False)
    occurred_on = Column(DateTime(timezone=True), nullable=False)
    payload = Column(JSONBCompat(), nullable=False)

    # State fields
    status = Column(String(50), nullable=False, default="pending")
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)

    # Distributed tracing fields
    correlation_id = Column(String(255), nullable=True)
    causation_id = Column(String(255), nullable=True)

    # Timestamp fields
    received_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at = Column(DateTime(timezone=True), nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_event_inbox_event_id"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'failed', 'dead_letter')",
            name="chk_event_inbox_status",
        ),
        Index("ix_event_inbox_status_next_retry", "status", "next_retry_at"),
        Index("ix_event_inbox_aggregate_received", "aggregate_id", "received_at"),
    )


class EventInbox(EventInboxMixin, Base):
    """
    Standalone EventInbox model using EventFlow's default Base.

    Use this directly if you don't need to integrate with an existing
    database schema. For custom integration, use EventInboxMixin instead.
    """

    pass


class SQLAlchemyInboxRepository:
    """
    SQLAlchemy implementation of InboxRepository protocol.

    Provides database persistence with:
    - Idempotent insertion (duplicate event_id returns None)
    - Row-level locking for concurrent worker coordination
    - Optimistic concurrency via SELECT FOR UPDATE
    - Dead letter queue management with replay support

    Uses SELECT FOR UPDATE SKIP LOCKED for efficient concurrent processing
    across multiple worker instances.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy async session for database operations
        """
        self._session = session

    async def insert_pending(self, entry: InboxEntry) -> Optional[InboxEntry]:
        """
        Insert a new entry in pending state.

        Provides idempotent insertion - if event_id already exists,
        returns None instead of raising an error.

        Args:
            entry: InboxEntry to insert

        Returns:
            The inserted entry, or None if event_id already exists
        """
        row = EventInbox(
            id=entry.id,
            event_id=entry.event_id,
            source_id=entry.source_id,
            event_type=entry.event_type,
            aggregate_id=entry.aggregate_id,
            correlation_id=entry.correlation_id,
            causation_id=entry.causation_id,
            occurred_on=entry.occurred_on,
            payload=entry.payload,
            max_retries=entry.max_retries,
        )
        self._session.add(row)

        try:
            await self._session.flush()
            return self._to_entity(row)
        except IntegrityError:
            await self._session.rollback()
            return None

    async def get_by_id(self, entry_id: UUID) -> Optional[InboxEntry]:
        """
        Retrieve an entry by its unique ID.

        Args:
            entry_id: The inbox entry's UUID

        Returns:
            The entry if found, None otherwise
        """
        stmt = select(EventInbox).where(EventInbox.id == entry_id)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_entity(row) if row else None

    async def save(self, entry: InboxEntry) -> None:
        """
        Persist changes to an existing entry.

        Uses SELECT FOR UPDATE to prevent lost updates from
        concurrent modifications.

        Args:
            entry: Modified InboxEntry to persist
        """
        stmt = select(EventInbox).where(EventInbox.id == entry.id).with_for_update()
        result = await self._session.execute(stmt)
        row = result.scalar_one()
        self._update_row(row, entry)
        await self._session.flush()

    async def acquire_due_entries(self, limit: int) -> List[InboxEntry]:
        """
        Acquire entries that are ready for processing.

        Uses SELECT FOR UPDATE SKIP LOCKED to ensure each entry is
        processed by exactly one worker in a concurrent environment.

        Returns entries that are:
        - PENDING status, or
        - FAILED status with next_retry_at <= now

        Args:
            limit: Maximum number of entries to acquire

        Returns:
            List of entries ready for processing
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
        return [self._to_entity(row) for row in result.scalars().all()]

    async def list_dead_lettered(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: Optional[str] = None,
    ) -> List[InboxEntry]:
        """
        List dead-lettered entries for inspection or replay.

        Args:
            limit: Maximum number of entries to return
            offset: Number of entries to skip (for pagination)
            event_type: Optional filter by event type

        Returns:
            List of dead-lettered entries, ordered by last_error_at desc

        Raises:
            ValueError: If limit <= 0 or offset < 0
        """
        if limit <= 0:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        query = (
            select(EventInbox)
            .where(EventInbox.status == "dead_letter")
            .order_by(EventInbox.last_error_at.desc())
            .limit(limit)
            .offset(offset)
        )

        if event_type:
            query = query.where(EventInbox.event_type == event_type)

        result = await self._session.execute(query)
        return [self._to_entity(row) for row in result.scalars().all()]

    async def replay_dead_lettered(self, event_ids: List[str]) -> int:
        """
        Reset dead-lettered entries for replay.

        Uses FOR UPDATE to prevent concurrent replay operations.
        Only resets entries currently in dead_letter status.

        Args:
            event_ids: List of event IDs to replay

        Returns:
            Number of entries successfully reset
        """
        if not event_ids:
            return 0

        stmt = (
            select(EventInbox)
            .where(
                EventInbox.event_id.in_(event_ids),
                EventInbox.status == "dead_letter",
            )
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())

        for row in rows:
            row.status = "pending"
            row.retry_count = 0
            row.next_retry_at = None
            row.error_message = None
            row.last_error_at = None

        await self._session.flush()
        return len(rows)

    def _to_entity(self, row: EventInbox) -> InboxEntry:
        """Convert ORM row to domain entity."""
        return InboxEntry(
            id=row.id,
            event_id=row.event_id,
            source_id=row.source_id,
            event_type=row.event_type,
            aggregate_id=row.aggregate_id,
            occurred_on=row.occurred_on,
            payload=row.payload,
            status=InboxStatus(row.status),
            retry_count=row.retry_count,
            max_retries=row.max_retries,
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            received_at=row.received_at,
            processed_at=row.processed_at,
            next_retry_at=row.next_retry_at,
            error_message=row.error_message,
            last_error_at=row.last_error_at,
        )

    def _update_row(self, row: EventInbox, entry: InboxEntry) -> None:
        """Update ORM row from domain entity."""
        row.status = entry.status.value
        row.retry_count = entry.retry_count
        row.processed_at = entry.processed_at
        row.next_retry_at = entry.next_retry_at
        row.error_message = entry.error_message
        row.last_error_at = entry.last_error_at
