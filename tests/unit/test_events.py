"""Tests for core event models."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from eventflow.core.events import BaseEvent, EventMetadata


def test_base_event_creation():
    """Test creating a BaseEvent."""
    event = BaseEvent(
        event_id="test-123", event_type="TestEvent", aggregate_id=uuid4(), payload={"key": "value"}
    )

    assert event.event_id == "test-123"
    assert event.event_type == "TestEvent"
    assert isinstance(event.aggregate_id, UUID)
    assert event.payload == {"key": "value"}


def test_base_event_defaults():
    """Test BaseEvent with default values."""
    event = BaseEvent()

    assert event.event_id  # Should be auto-generated
    assert isinstance(event.aggregate_id, UUID)
    assert isinstance(event.occurred_on, datetime)
    assert event.payload == {}


def test_base_event_metadata():
    """Test extracting metadata from event."""
    aggregate_id = uuid4()
    event = BaseEvent(event_id="test-123", event_type="TestEvent", aggregate_id=aggregate_id)

    metadata = event.metadata

    assert isinstance(metadata, EventMetadata)
    assert metadata.event_id == "test-123"
    assert metadata.event_type == "TestEvent"
    assert metadata.aggregate_id == aggregate_id


def test_base_event_to_dict():
    """Test converting event to dictionary."""
    aggregate_id = uuid4()
    occurred_on = datetime.now(tz=timezone.utc)

    event = BaseEvent(
        event_id="test-123",
        event_type="TestEvent",
        aggregate_id=aggregate_id,
        occurred_on=occurred_on,
        payload={"key": "value"},
    )

    data = event.to_dict()

    assert data["event_id"] == "test-123"
    assert data["event_type"] == "TestEvent"
    assert data["aggregate_id"] == str(aggregate_id)
    assert data["occurred_on"] == occurred_on.isoformat()
    assert data["payload"] == {"key": "value"}


def test_base_event_from_dict():
    """Test creating event from dictionary."""
    aggregate_id = uuid4()
    data = {
        "event_id": "test-123",
        "event_type": "TestEvent",
        "aggregate_id": str(aggregate_id),
        "occurred_on": "2025-01-01T00:00:00+00:00",
        "payload": {"key": "value"},
    }

    event = BaseEvent.from_dict(data)

    assert event.event_id == "test-123"
    assert event.event_type == "TestEvent"
    assert event.aggregate_id == aggregate_id
    assert event.payload == {"key": "value"}
