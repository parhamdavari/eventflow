<div align="center">

<h1>EventFlow</h1>

<p>Transactional Inbox pattern for Python microservices — Redis Streams + SQLAlchemy, no framework required.</p>

[![PyPI version](https://badge.fury.io/py/python-eventflow.svg)](https://pypi.org/project/python-eventflow/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

---

```mermaid
flowchart LR
    Producer --> Stream[(Redis Stream)]

    subgraph Workers["Consumer Group"]
        C1[InboxConsumer]
        C2[InboxConsumer]
    end

    Stream -->|XREADGROUP| C1 & C2
    C1 & C2 -->|insert_pending| DB[(event_inbox)]
    DB -->|acquire_due_events| C1 & C2
    C1 & C2 --> Handler[handle_event]
    Handler -->|ok: mark_processed| DB
    Handler -->|err: retry / dead_letter| DB
```

---

## Quick Start

```bash
pip install python-eventflow asyncpg
```

```python
from eventflow import InboxConsumer, RedisStreamsTransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

class Handlers:
    async def handle_event(self, session, inbox):
        print(inbox.event_type, inbox.payload)

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
redis = RedisStreamsTransport(host="localhost", port=6379).build_client()

consumer = InboxConsumer(
    redis_client=redis,
    session_factory=async_sessionmaker(engine, expire_on_commit=False),
    stream_name="my-events",
    consumer_group="my-service",
    consumer_name_prefix="worker",
    event_handlers=Handlers(),
)

await consumer.start()
```

---

## Layout

```
eventflow/
├── core/         Pure abstractions (events, protocols, types)
├── transports/   Transport implementations (Redis Streams)
├── patterns/
│   ├── inbox/    Transactional Inbox — consumer, models, repository
│   └── outbox/   Transactional Outbox (coming soon)
└── utils/        Exception hierarchy
```

## Development

| Command | Purpose |
|---|---|
| `poetry run pytest` | Run tests |
| `poetry run mypy eventflow` | Type check |
| `poetry run black eventflow tests` | Format |
| `poetry run ruff check eventflow tests` | Lint |

## License

MIT. See `LICENSE`.
