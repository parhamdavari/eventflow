"""
Redis Streams transport implementation.

Extracted and generified from rasa-mach redis_streams.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis


class RedisStreamsTransport:
    """
    Redis Streams transport for EventFlow.

    Provides connection management and basic operations for Redis Streams.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
        decode_responses: bool = True,
        socket_keepalive: bool = True,
        socket_timeout: int = 10,
        health_check_interval: int = 30,
    ):
        """
        Initialize Redis Streams transport.

        Args:
            host: Redis server host
            port: Redis server port
            password: Redis password (if required)
            db: Redis database number
            decode_responses: Whether to decode responses to strings
            socket_keepalive: Enable socket keepalive
            socket_timeout: Socket timeout in seconds
            health_check_interval: Health check interval in seconds
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.decode_responses = decode_responses
        self.socket_keepalive = socket_keepalive
        self.socket_timeout = socket_timeout
        self.health_check_interval = health_check_interval
        self._client: redis.Redis | None = None

    def build_client(self) -> redis.Redis:
        """
        Build a Redis client configured for stream operations.

        Returns:
            Configured Redis client
        """
        return redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db,
            decode_responses=self.decode_responses,
            socket_keepalive=self.socket_keepalive,
            socket_timeout=self.socket_timeout,
            health_check_interval=self.health_check_interval,
        )

    @asynccontextmanager
    async def client(self) -> AsyncIterator[redis.Redis]:
        """
        Async context manager that yields a Redis client and closes it afterwards.

        Usage:
            async with transport.client() as client:
                await client.xadd(...)
        """
        client = self.build_client()
        try:
            yield client
        finally:
            await client.aclose()

    async def close(self) -> None:
        """Close any open connections."""
        if self._client:
            await self._client.aclose()
            self._client = None
