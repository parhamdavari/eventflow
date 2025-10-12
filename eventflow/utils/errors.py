"""Exception hierarchy for EventFlow."""


class EventFlowError(Exception):
    """Base exception for all EventFlow errors."""

    pass


class TransportError(EventFlowError):
    """Raised when transport operations fail."""

    pass


class InboxError(EventFlowError):
    """Raised when inbox operations fail."""

    pass


class OutboxError(EventFlowError):
    """Raised when outbox operations fail."""

    pass


class SerializationError(EventFlowError):
    """Raised when event serialization/deserialization fails."""

    pass
