"""
In-memory adapter for Inbox persistence.

Provides a fast, isolated implementation for unit tests.
No external dependencies - tests run instantly without database setup.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

from ..entity import InboxEntry, InboxStatus


class InMemoryInboxRepository:
    """
    In-memory implementation of InboxRepository for testing.

    Provides the same interface as SQLAlchemyInboxRepository but stores
    entries in memory. Useful for:
    - Fast unit tests without database dependencies
    - Integration tests where inbox behavior needs isolation
    - Local development without PostgreSQL

    Thread-safe for async tests (single-threaded async execution).
    """

    def __init__(self):
        """Initialize empty in-memory storage."""
        self._entries: Dict[UUID, InboxEntry] = {}
        self._by_event_id: Dict[str, UUID] = {}

    async def insert_pending(self, entry: InboxEntry) -> Optional[InboxEntry]:
        """
        Insert entry, return None if duplicate event_id.

        Simulates the unique constraint on event_id.
        """
        if entry.event_id in self._by_event_id:
            return None
        self._entries[entry.id] = entry
        self._by_event_id[entry.event_id] = entry.id
        return entry

    async def get_by_id(self, entry_id: UUID) -> Optional[InboxEntry]:
        """Get entry by ID."""
        return self._entries.get(entry_id)

    async def save(self, entry: InboxEntry) -> None:
        """Persist entry changes."""
        self._entries[entry.id] = entry

    async def acquire_due_entries(self, limit: int) -> List[InboxEntry]:
        """
        Acquire entries ready for processing.

        Returns pending entries and failed entries with due retries,
        sorted by received_at (oldest first).
        """
        now = datetime.now(tz=timezone.utc)
        due = [
            e
            for e in self._entries.values()
            if e.status in (InboxStatus.PENDING, InboxStatus.FAILED)
            and (e.next_retry_at is None or e.next_retry_at <= now)
        ]
        due.sort(key=lambda e: e.received_at)
        return due[:limit]

    async def list_dead_lettered(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: Optional[str] = None,
    ) -> List[InboxEntry]:
        """
        List dead-lettered entries for inspection.

        Returns entries sorted by last_error_at (most recent first).
        """
        if limit <= 0:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        dead = [
            e
            for e in self._entries.values()
            if e.status == InboxStatus.DEAD_LETTER
            and (event_type is None or e.event_type == event_type)
        ]
        dead.sort(key=lambda e: e.last_error_at or e.received_at, reverse=True)
        return dead[offset : offset + limit]

    async def replay_dead_lettered(self, event_ids: List[str]) -> int:
        """
        Reset dead-lettered entries for replay.

        Only resets entries currently in DEAD_LETTER status.
        """
        count = 0
        for event_id in event_ids:
            if event_id in self._by_event_id:
                entry = self._entries[self._by_event_id[event_id]]
                if entry.status == InboxStatus.DEAD_LETTER:
                    entry.status = InboxStatus.PENDING
                    entry.retry_count = 0
                    entry.next_retry_at = None
                    entry.error_message = None
                    entry.last_error_at = None
                    count += 1
        return count

    def clear(self) -> None:
        """Clear all entries (for test cleanup)."""
        self._entries.clear()
        self._by_event_id.clear()

    def count(self) -> int:
        """Return total number of entries (test helper)."""
        return len(self._entries)

    def get_by_status(self, status: InboxStatus) -> List[InboxEntry]:
        """Get all entries with given status (test helper)."""
        return [e for e in self._entries.values() if e.status == status]
