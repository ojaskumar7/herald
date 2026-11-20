"""Exceptions raised by Herald."""

from __future__ import annotations


class HeraldError(Exception):
    """Base class for all Herald errors."""


class BusClosedError(HeraldError, RuntimeError):
    """Raised when an operation requires an open bus."""


class SubscriberDefinitionError(HeraldError, TypeError):
    """Raised when a ``@subscribe`` method has an invalid definition."""
