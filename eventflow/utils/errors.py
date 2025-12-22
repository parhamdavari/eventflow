"""Exception hierarchy for EventFlow."""


class EventFlowError(Exception):
    """Base exception for all EventFlow errors."""


class TransportError(EventFlowError):
    """Raised when transport operations fail."""


class InboxError(EventFlowError):
    """Raised when inbox operations fail."""


class OutboxError(EventFlowError):
    """Raised when outbox operations fail."""


class SerializationError(EventFlowError):
    """Raised when event serialization/deserialization fails."""
