"""
Event Inbox SQLAlchemy model.

Extracted and generified from rasa-mach workflow_event_inbox.py.
Provides persistent storage for events before business processing.
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy.types import JSON, TypeDecorator


class Base(DeclarativeBase):
    pass


class JSONBCompat(TypeDecorator):
    """
    Compat wrapper for JSON payloads.

    Uses PostgreSQL JSONB when available, falls back to generic JSON for
    SQLite so unit tests can execute without a PostgreSQL dependency.
    """

    impl = JSONB
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB(astext_type=Text()))
        return dialect.type_descriptor(JSON())


class EventInbox(Base):
    """
    Transactional inbox for event storage.

    Stores events from Redis Streams before business processing to ensure
    exactly-once processing semantics and reliable recovery.
    """

    __tablename__ = "event_inbox"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    stream_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    occurred_on: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONBCompat(), nullable=False)

    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_error_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_event_inbox_event_id"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'failed', 'dead_letter')",
            name="chk_event_inbox_status",
        ),
        Index("ix_event_inbox_status_next_retry", "status", "next_retry_at"),
        Index("ix_event_inbox_aggregate_received", "aggregate_id", "received_at"),
    )

    def mark_processing(self) -> None:
        """Transition event to processing state."""
        self.status = "processing"
        self.processed_at = None
        self.next_retry_at = None

    def mark_processed(self) -> None:
        """Mark event as successfully processed."""
        self.status = "processed"
        self.processed_at = datetime.now(tz=timezone.utc)
        self.next_retry_at = None
        self.error_message = None
        self.last_error_at = None
        self.retry_count = 0

    def record_failure(
        self,
        message: str,
        *,
        retry_at: datetime | None,
        retry_count: int,
        dead_letter: bool = False,
    ) -> None:
        """
        Record processing failure and schedule retry or dead-letter.

        Args:
            message: Error message
            retry_at: When to retry (None if not retrying)
            retry_count: Current retry count
            dead_letter: True when event should no longer be retried
        """
        self.status = "dead_letter" if dead_letter else "failed"
        self.error_message = message[:1000] if message else None
        self.last_error_at = datetime.now(tz=timezone.utc)
        self.next_retry_at = retry_at
        self.retry_count = retry_count
