<h1 align="center">EventFlow</h1>

<p align="center">
  Production-ready event-driven infrastructure for Python microservices.<br/>
  Reliable consumption with the <strong>Transactional Inbox</strong> pattern on top of <strong>Redis Streams</strong> + <strong>SQLAlchemy</strong>.
</p>

---

[![PyPI version](https://badge.fury.io/py/python-eventflow.svg)](https://pypi.org/project/python-eventflow/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`eventflow` is a small, battle-tested toolkit for building reliable event-driven services. It focuses on the **consumer side**: it ingests events from Redis Streams, stores them in a durable inbox table, and processes them with retries and dead-lettering.

> Note: the producer-side **Transactional Outbox** is intentionally not implemented yet (`OutboxPublisher` raises `NotImplementedError`).

###

### Quick Start

Install:

```bash
pip install python-eventflow asyncpg
```

`asyncpg` is the PostgreSQL async driver used in the examples; you can use a different SQLAlchemy async driver if needed.

Create the inbox table (standalone usage):

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventflow.patterns.inbox.models import Base

engine = create_async_engine("postgresql+asyncpg://postgres:1234@localhost:5432/mydb")

async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)
```

Run a consumer:

```python
from eventflow import InboxConsumer, RedisStreamsTransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

class Handlers:
    async def handle_event(self, session, inbox):
        print(inbox.event_type, inbox.payload)

engine = create_async_engine("postgresql+asyncpg://postgres:1234@localhost:5432/mydb")
Session = async_sessionmaker(engine, expire_on_commit=False)

redis = RedisStreamsTransport(host="localhost", port=6379).build_client()

consumer = InboxConsumer(
    redis_client=redis,
    session_factory=Session,
    stream_name="my-events",
    consumer_group="my-service",
    consumer_name_prefix="worker",
    event_handlers=Handlers(),
)

await consumer.start()
```

### Features

*   **Transactional Inbox (Exactly-once processing)**: idempotent persistence keyed by `event_id`.
*   **Redis Streams transport**: consumer groups + acknowledgement handling.
*   **Safe concurrency**: workers cooperate via `SELECT ... FOR UPDATE SKIP LOCKED`.
*   **Retries + dead-lettering**: exponential backoff capped at 15 minutes.
*   **Type-safe event model**: `BaseEvent` + `EventMetadata`, full type hints and mypy support.
*   **SQLite-friendly tests**: JSON payloads fall back cleanly for unit tests (`JSONBCompat`).

### Architecture Flow

```mermaid
flowchart LR
    Producer[Producer] --> Stream[(Redis Stream)]

    subgraph Consumers["Consumers (same consumer group)"]
        C1[InboxConsumer]
        C2[InboxConsumer]
    end

    Stream -->|XREADGROUP| C1
    Stream -->|XREADGROUP| C2

    C1 -->|insert_pending<br/>(idempotent)| Inbox[(event_inbox)]
    C2 -->|insert_pending<br/>(idempotent)| Inbox

    Inbox -->|acquire_due_events<br/>(SKIP LOCKED)| C1
    Inbox -->|acquire_due_events<br/>(SKIP LOCKED)| C2

    C1 --> Handler[Your handler<br/>handle_event(session, inbox)]
    C2 --> Handler

    Handler -->|success| Inbox
    Handler -->|failure<br/>schedule retry / dead-letter| Inbox
```

### Event Format

The consumer supports two common Redis Stream payload styles:

1) A single `data` field containing JSON (recommended):

```bash
redis-cli XADD my-events '*' data '{"event_id":"e-1","event_type":"OrderCreated","aggregate_id":"7c8f0a6a-7b7c-4c74-9cfb-2e2e2b9b1d33","occurred_on":"2025-01-01T00:00:00Z","payload":{"order_id":"o-123"}}'
```

2) A “flattened” entry with top-level fields (`event_id`, `event_type`, ...).

### Schema Options

If you already have your own SQLAlchemy `Base`, use the mixin:

```python
from eventflow.patterns.inbox.models import EventInboxMixin

class EventInbox(EventInboxMixin, YourBase):
    __tablename__ = "event_inbox"
```

<details>
  <summary>PostgreSQL SQL schema (reference)</summary>

```sql
-- For gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE event_inbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL UNIQUE,
    stream_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    aggregate_id UUID NOT NULL,
    correlation_id VARCHAR(255),
    occurred_on TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    last_error_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT chk_event_inbox_status CHECK (
        status IN ('pending', 'processing', 'processed', 'failed', 'dead_letter')
    )
);

CREATE UNIQUE INDEX uq_event_inbox_event_id ON event_inbox(event_id);
CREATE INDEX ix_event_inbox_status_next_retry ON event_inbox(status, next_retry_at);
CREATE INDEX ix_event_inbox_aggregate_received ON event_inbox(aggregate_id, received_at);
```
</details>

### Configuration & Tuning

- Batch size: `InboxConsumer.BATCH_SIZE` (default: `10`)
- Read block time: `InboxConsumer.BLOCK_MS` (default: `1000`)
- Retry policy: stored per row (`max_retries`, `retry_count`, `next_retry_at`); backoff is exponential and capped at 15 minutes.

### Development

```bash
poetry install
poetry run pytest
poetry run mypy eventflow
poetry run black --check eventflow tests
```

### License

MIT License. See `LICENSE`.
