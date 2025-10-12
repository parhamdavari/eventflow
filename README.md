# EventFlow

**Production-ready event-driven architecture toolkit for Python microservices.**

EventFlow provides battle-tested infrastructure components for building reliable event-driven systems using the **Transactional Inbox/Outbox patterns**.

## 🎯 Features

- ✅ **Transactional Inbox Pattern** - Reliable event consumption with exactly-once semantics
- ✅ **Redis Streams Transport** - Production-ready transport with consumer groups
- ✅ **Automatic Retries** - Exponential backoff with configurable dead-letter handling
- ✅ **Concurrent Processing** - Multiple workers with `SELECT FOR UPDATE SKIP LOCKED`
- ✅ **Type-Safe** - Full type hints and mypy support
- ✅ **Battle-Tested** - Extracted from production code at rasa-mach
- 🚧 **Transactional Outbox Pattern** - Coming soon (producer side)

## 📦 Installation

```bash
pip install eventflow
```

## 🚀 Quick Start

### Consumer Side (Inbox Pattern)

```python
from eventflow import InboxConsumer
from eventflow.transports import RedisStreamsTransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Your business logic handlers
class MyEventHandlers:
    async def handle_event(self, session, inbox):
        # Process the event
        print(f"Processing {inbox.event_type}: {inbox.payload}")

# Setup
engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
redis_client = RedisStreamsTransport(host="localhost").build_client()

# Create consumer
consumer = InboxConsumer(
    redis_client=redis_client,
    session_factory=session_factory,
    stream_name="my-events",
    consumer_group="my-service",
    consumer_name_prefix="worker",
    event_handlers=MyEventHandlers()
)

# Start consuming
await consumer.start()
```

### Database Schema

EventFlow requires an `event_inbox` table. Create it with:

```python
from eventflow.patterns.inbox.models import Base

# Create tables
async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)
```

Or use this SQL:

```sql
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

## 📚 Architecture

EventFlow follows a layered architecture:

```
eventflow/
├── core/           # Pure abstractions (BaseEvent, protocols)
├── transports/     # Pluggable transports (Redis Streams, future: Kafka)
├── patterns/       # Reliability patterns (Inbox, Outbox)
│   ├── inbox/      # Consumer-side reliability
│   └── outbox/     # Producer-side reliability (coming soon)
└── utils/          # Utilities and errors
```

### Design Principles

1. **Dependency Inversion** - Core has no external dependencies
2. **Open/Closed** - Easy to extend with new transports
3. **Single Responsibility** - Each module has one job
4. **Battle-Tested** - Extracted from production systems

## 🔄 How It Works

### Transactional Inbox Pattern

1. **Pull** events from Redis Streams
2. **Store** in database inbox (atomic, idempotent)
3. **Process** through business handlers
4. **Retry** on failure with exponential backoff
5. **Dead-letter** after max retries

```
Redis Stream → Inbox Table → Business Logic → Mark Processed
       ↓            ↓              ↓
    Durable    Idempotent    Exactly-once
```

### Reliability Guarantees

- **Exactly-once processing** - Duplicate detection via event_id
- **At-least-once delivery** - Redis Streams with consumer groups
- **Automatic recovery** - Failed events retry with backoff
- **Concurrent processing** - Multiple workers cooperate safely

## 🛠️ Configuration

### Retry Strategy

```python
# Default: 3 retries with exponential backoff
# Backoff: 60s, 120s, 240s (capped at 15 minutes)

# Customize in EventInbox model
inbox.max_retries = 5  # Increase retries
```

### Batch Size

```python
# Default: 10 events per batch
consumer.BATCH_SIZE = 20  # Process more events per batch
```

### Consumer Groups

Multiple workers can share the load:

```python
# Worker 1
consumer = InboxConsumer(..., consumer_name_prefix="worker-1")

# Worker 2
consumer = InboxConsumer(..., consumer_name_prefix="worker-2")

# Both workers read from the same stream in parallel
```

## 🧪 Testing

EventFlow is thoroughly tested. Run tests:

```bash
poetry install
poetry run pytest
```

## 📖 Documentation

- [Architecture Guide](docs/architecture.md)
- [API Reference](docs/api.md)
- [Migration from rasa-mach](docs/migration.md)

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run `poetry run pytest` and `poetry run mypy`
5. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) file.

## 🙏 Credits

Extracted from production code at **rasa-mach** project. 
Battle-tested in real-world microservices architecture.

## 🔮 Roadmap

- [x] Transactional Inbox pattern
- [x] Redis Streams transport
- [ ] Transactional Outbox pattern (producer side)
- [ ] Kafka transport
- [ ] Observability hooks (metrics, tracing)
- [ ] CloudEvents support
- [ ] Dead-letter queue management UI

---

**Made with ❤️ for reliable distributed systems**

