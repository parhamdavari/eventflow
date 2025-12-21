# Transactional Inbox Pattern

> Reliable event consumption with exactly-once processing semantics.

## What Problem Does This Solve?

When consuming events from a message broker, you face the **dual-write problem**:

1. You read an event from the broker
2. You process it (update database, call APIs)
3. You acknowledge the event

**What if your service crashes between steps 2 and 3?**

- If you acknowledge first, you might lose the event
- If you process first, you might process the same event twice

The **Transactional Inbox** pattern solves this by:

1. **Storing events durably** in your database before processing
2. **Processing from the database**, not directly from the broker
3. **Using database transactions** to ensure exactly-once semantics

## Architecture

This module follows **Ports & Adapters (Hexagonal) Architecture**:

```
inbox/
├── entity.py          # Pure domain entity (no dependencies)
├── ports.py           # Abstract interfaces (protocols)
├── processor.py       # Application service (orchestration)
├── dlq.py             # Dead Letter Queue value object
└── adapters/
    ├── sqlalchemy.py  # Repository: PostgreSQL/SQLite
    ├── memory.py      # Repository: Testing
    └── redis.py       # Transport: Redis Streams
```

### Why This Architecture?

| Layer | Responsibility | Benefit |
|-------|----------------|---------|
| **Entity** | Business rules, state machine | Testable without database |
| **Ports** | Contract definitions | Swap implementations freely |
| **Adapters** | Technical implementations | Isolate framework dependencies |
| **Processor** | Orchestration | Coordinates domain + infrastructure |

### Dependency Direction

```
┌─────────────────────────────────────────┐
│              Application                │
│  ┌─────────────────────────────────┐   │
│  │         processor.py            │   │
│  │    (orchestrates everything)    │   │
│  └──────────┬──────────┬───────────┘   │
│             │          │               │
│     ┌───────▼───┐  ┌───▼───────┐      │
│     │  ports.py │  │ entity.py │      │
│     │(interfaces)│ │  (domain) │      │
│     └───────────┘  └───────────┘      │
└─────────────────────────────────────────┘
                    ▲
                    │ implements
┌─────────────────────────────────────────┐
│              Adapters                   │
│  ┌────────────┐  ┌────────────────┐    │
│  │ sqlalchemy │  │     redis      │    │
│  │ (database) │  │  (transport)   │    │
│  └────────────┘  └────────────────┘    │
└─────────────────────────────────────────┘
```

## Quick Start

```python
from eventflow.patterns.inbox import (
    InboxProcessor,
    SQLAlchemyInboxRepository,
    RedisStreamTransport,
    InboxEntry,
    EventHandler,
)

# 1. Implement your event handler
class OrderEventHandler:
    async def handle(self, entry: InboxEntry, session) -> None:
        if entry.event_type == "OrderCreated":
            order_id = entry.payload["order_id"]
            # Your business logic here
            await self.create_shipment(order_id, session)

# 2. Create the processor
processor = InboxProcessor(
    transport=RedisStreamTransport(
        redis_client=redis,
        stream_name="order_events",
        consumer_group="shipping-service",
    ),
    session_factory=async_session,
    repository_class=SQLAlchemyInboxRepository,
    event_handler=OrderEventHandler(),
)

# 3. Run the processor
await processor.start()
```

## Event Lifecycle

```
┌─────────┐     ┌────────────┐     ┌───────────┐
│ PENDING │────▶│ PROCESSING │────▶│ PROCESSED │
└─────────┘     └────────────┘     └───────────┘
                      │
                      ▼ (on error)
                ┌──────────┐     ┌─────────────┐
                │  FAILED  │────▶│ DEAD_LETTER │
                └──────────┘     └─────────────┘
                      │
                      └────(retry)──────┘
```

## Processing Flow

```
Transport (Redis)          Inbox (Database)           Handler
      │                          │                       │
      │  1. XREADGROUP           │                       │
      │◄─────────────────────    │                       │
      │                          │                       │
      │  2. Store as PENDING     │                       │
      │────────────────────────►│                       │
      │                          │                       │
      │  3. XACK (durability)    │                       │
      │◄─────────────────────    │                       │
      │                          │                       │
      │                          │  4. Acquire due       │
      │                          │─────────────────────►│
      │                          │                       │
      │                          │  5. Process           │
      │                          │◄─────────────────────│
      │                          │                       │
      │                          │  6. Mark PROCESSED    │
      │                          │◄─────────────────────│
```

## Dead Letter Queue

Events that fail after `max_retries` are dead-lettered:

```python
from eventflow.patterns.inbox import InboxDLQEvent

# List dead-lettered events
dead = await repo.list_dead_lettered(limit=10)

# Convert to DLQ event for alerting
for entry in dead:
    dlq_event = InboxDLQEvent.from_entry(entry)
    await send_to_alerting(dlq_event.to_dict())

# Replay after fixing root cause
count = await repo.replay_dead_lettered(["event-1", "event-2"])
```

## Testing with In-Memory Adapter

```python
from eventflow.patterns.inbox import InMemoryInboxRepository

# Fast unit tests without database
repo = InMemoryInboxRepository()
entry = await repo.insert_pending(test_entry)
assert entry is not None
```

## Extending

### Custom Transport (e.g., Kafka)

Implement the `MessageTransport` protocol:

```python
from eventflow.patterns.inbox.ports import MessageTransport, RawMessage

class KafkaTransport:
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def receive_batch(self, batch_size, timeout_ms) -> List[RawMessage]: ...
    async def acknowledge(self, source_id: str) -> None: ...
    @property
    def consumer_name(self) -> str: ...
```

### Custom Retry Policy

```python
from eventflow.patterns.inbox.ports import RetryPolicy
from datetime import datetime, timedelta, timezone

class LinearBackoff:
    def calculate_next_retry(self, retry_count: int) -> datetime:
        return datetime.now(tz=timezone.utc) + timedelta(minutes=retry_count * 5)

processor = InboxProcessor(
    ...,
    retry_policy=LinearBackoff(),
)
```

### Custom Repository (e.g., MongoDB)

Implement the `InboxRepository` protocol:

```python
from eventflow.patterns.inbox.ports import InboxRepository

class MongoInboxRepository:
    async def insert_pending(self, entry): ...
    async def get_by_id(self, entry_id): ...
    async def save(self, entry): ...
    async def acquire_due_entries(self, limit): ...
    async def list_dead_lettered(self, limit, offset, event_type): ...
    async def replay_dead_lettered(self, event_ids): ...
```

## Key Concepts

### InboxEntry (Domain Entity)
Pure Python dataclass with state machine logic. No ORM dependencies.
Contains `correlation_id` and `causation_id` for distributed tracing.

### source_id
Transport-agnostic identifier for the message source. Could be:
- Redis Stream entry ID
- Kafka topic:partition:offset
- RabbitMQ delivery tag

### Ports vs Adapters
- **Ports** define WHAT operations are needed (interfaces)
- **Adapters** define HOW they're implemented (concrete classes)

## See Also

- [Transactional Outbox Pattern](../outbox/) - Producer-side complement
- [EventFlow Core](../../core/) - Base events and protocols
