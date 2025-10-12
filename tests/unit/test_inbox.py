"""Tests for inbox pattern."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from eventflow.core.events import BaseEvent
from eventflow.patterns.inbox.models import EventInbox
from eventflow.patterns.inbox.repository import EventInboxRepository


@pytest.mark.asyncio
async def test_insert_pending(session):
    """Test inserting event into inbox."""
    repo = EventInboxRepository(session)
    
    event = BaseEvent(
        event_id="test-123",
        event_type="TestEvent",
        aggregate_id=uuid4(),
        payload={"key": "value"}
    )
    
    inbox = await repo.insert_pending(event)
    
    assert inbox is not None
    assert inbox.event_id == "test-123"
    assert inbox.event_type == "TestEvent"
    assert inbox.status == "pending"
    assert inbox.retry_count == 0


@pytest.mark.asyncio
async def test_insert_duplicate_returns_none(session):
    """Test that inserting duplicate event_id returns None."""
    repo = EventInboxRepository(session)
    
    event = BaseEvent(
        event_id="test-123",
        event_type="TestEvent",
        aggregate_id=uuid4()
    )
    
    # First insert succeeds
    inbox1 = await repo.insert_pending(event)
    await session.commit()
    
    assert inbox1 is not None
    
    # Second insert returns None (duplicate)
    inbox2 = await repo.insert_pending(event)
    
    assert inbox2 is None


@pytest.mark.asyncio
async def test_mark_processing(session):
    """Test marking event as processing."""
    repo = EventInboxRepository(session)
    
    event = BaseEvent(event_id="test-123", event_type="TestEvent", aggregate_id=uuid4())
    inbox = await repo.insert_pending(event)
    await session.commit()
    
    await repo.mark_processing(inbox)
    await session.commit()
    
    assert inbox.status == "processing"


@pytest.mark.asyncio
async def test_mark_processed(session):
    """Test marking event as processed."""
    repo = EventInboxRepository(session)
    
    event = BaseEvent(event_id="test-123", event_type="TestEvent", aggregate_id=uuid4())
    inbox = await repo.insert_pending(event)
    await repo.mark_processing(inbox)
    await session.commit()
    
    await repo.mark_processed(inbox)
    await session.commit()
    
    assert inbox.status == "processed"
    assert inbox.processed_at is not None
    assert inbox.error_message is None


@pytest.mark.asyncio
async def test_acquire_due_events(session):
    """Test acquiring events ready for processing."""
    repo = EventInboxRepository(session)
    
    # Create pending event
    event = BaseEvent(event_id="test-123", event_type="TestEvent", aggregate_id=uuid4())
    await repo.insert_pending(event)
    await session.commit()
    
    # Acquire it
    due_events = await repo.acquire_due_events(limit=10)
    
    assert len(due_events) == 1
    assert due_events[0].event_id == "test-123"
    assert due_events[0].status == "pending"

