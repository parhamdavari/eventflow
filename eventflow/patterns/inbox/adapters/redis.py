"""
Redis Streams transport adapter for the Inbox pattern.

Implements MessageTransport protocol using Redis Streams with consumer groups
for distributed, reliable message consumption.

Features:
- Consumer group support for horizontal scaling
- Automatic consumer group creation
- Unique consumer names per instance (hostname + PID)
- Graceful connection handling
"""

from __future__ import annotations

import logging
import os
import socket
from typing import Any, Dict, List

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from ..ports import MessageTransport, RawMessage

logger = logging.getLogger(__name__)


class RedisStreamTransport:
    """
    Redis Streams implementation of MessageTransport.

    Uses Redis consumer groups to ensure each message is processed
    by exactly one consumer in a distributed deployment.

    Redis Streams specifics:
    - Messages are identified by stream entry IDs (e.g., "1234567890123-0")
    - Consumer groups track which messages have been delivered
    - XACK marks messages as processed
    - Unacknowledged messages can be reclaimed after timeout
    """

    def __init__(
        self,
        redis_client: Redis,
        stream_name: str,
        consumer_group: str,
        consumer_name_prefix: str = "inbox",
    ) -> None:
        """
        Initialize Redis transport.

        Args:
            redis_client: Async Redis client instance
            stream_name: Redis stream key to consume from
            consumer_group: Consumer group name for distributed processing
            consumer_name_prefix: Prefix for unique consumer identification
        """
        self._redis = redis_client
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._consumer_name = (
            f"{consumer_name_prefix}-{socket.gethostname()}-{os.getpid()}"
        )

    async def connect(self) -> None:
        """
        Create consumer group if it doesn't exist.

        Uses XGROUP CREATE with MKSTREAM to create both the stream
        and consumer group atomically if needed.
        """
        try:
            await self._redis.xgroup_create(
                name=self._stream_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Created Redis consumer group '%s' on stream '%s'",
                self._consumer_group,
                self._stream_name,
            )
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    "Consumer group '%s' already exists",
                    self._consumer_group,
                )
            else:
                raise

    async def disconnect(self) -> None:
        """Close Redis connection gracefully."""
        await self._redis.aclose()
        logger.info("Redis transport disconnected")

    async def receive_batch(
        self,
        batch_size: int,
        timeout_ms: int,
    ) -> List[RawMessage]:
        """
        Read messages from Redis stream using XREADGROUP.

        Uses the ">" special ID to read only new messages that haven't
        been delivered to any consumer in the group.

        Args:
            batch_size: Maximum messages to read (COUNT parameter)
            timeout_ms: Block timeout (BLOCK parameter)

        Returns:
            List of RawMessage with source_id (stream entry ID) and data
        """
        try:
            messages = await self._redis.xreadgroup(
                groupname=self._consumer_group,
                consumername=self._consumer_name,
                streams={self._stream_name: ">"},
                count=batch_size,
                block=timeout_ms,
            )
        except ResponseError as exc:
            logger.error("Redis XREADGROUP error: %s", exc)
            return []

        if not messages:
            return []

        result: List[RawMessage] = []
        for _stream_key, entries in messages:
            for stream_id, entry_data in entries:
                # Convert bytes keys/values to strings if needed
                data = self._normalize_entry(entry_data)
                result.append(RawMessage(source_id=stream_id, data=data))

        return result

    async def acknowledge(self, source_id: str) -> None:
        """
        Acknowledge message processing with XACK.

        After acknowledgment, Redis will not redeliver this message
        to any consumer in the group.
        """
        await self._redis.xack(self._stream_name, self._consumer_group, source_id)

    @property
    def consumer_name(self) -> str:
        """Unique consumer identifier for this instance."""
        return self._consumer_name

    def _normalize_entry(self, entry_data: Dict[Any, Any]) -> Dict[str, Any]:
        """
        Normalize Redis entry data to string keys.

        Redis returns bytes when connection is not decode_responses=True.
        This ensures consistent Dict[str, Any] output.
        """
        normalized: Dict[str, Any] = {}
        for key, value in entry_data.items():
            str_key = key.decode() if isinstance(key, bytes) else str(key)
            if isinstance(value, bytes):
                normalized[str_key] = value.decode()
            else:
                normalized[str_key] = value
        return normalized
