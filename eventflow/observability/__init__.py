"""
Cross-cutting observability components for EventFlow.

This module provides monitoring, logging, and metrics infrastructure
that can be used across all event-driven patterns (inbox, outbox, saga).

Example:
    from eventflow.observability import DLQMetrics, log_dead_letter

    metrics = DLQMetrics()
    await metrics.increment("OrderCreated")
"""

from eventflow.observability.dlq import DLQMetrics, log_dead_letter

__all__ = [
    "DLQMetrics",
    "log_dead_letter",
]
