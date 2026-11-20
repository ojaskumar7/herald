"""The Herald event bus."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, cast
from weakref import WeakKeyDictionary

from .errors import BusClosedError

T = TypeVar("T")
Handler = Callable[[Any], None]


@dataclass(frozen=True, slots=True)
class _Entry:
    event_type: type[Any]
    handler: Handler
    subscription: Subscription


class Subscription:
    """Handle for one handler registration."""

    __slots__ = ("_active", "_bus", "_event_type", "_handler")

    def __init__(self, bus: EventBus, event_type: type[Any], handler: Handler) -> None:
        self._bus = bus
        self._event_type = event_type
        self._handler = handler
        self._active = True

    @property
    def event_type(self) -> type[Any]:
        """The event type this subscription is registered for."""
        return self._event_type

    @property
    def active(self) -> bool:
        """Whether this subscription is still receiving events."""
        with self._bus._lock:
            return self._active

    def cancel(self) -> None:
        """Cancel this subscription. Idempotent."""
        self._bus._cancel(self)

    def __repr__(self) -> str:
        return f"Subscription(event_type={self._event_type.__name__!r}, active={self.active!r})"


class EventBus:
    """In-process publish/subscribe event bus."""

    def __init__(
        self,
        *,
        max_workers: int = 4,
        name: str = "herald",
        logger: logging.Logger | None = None,
    ) -> None:
        if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
            raise ValueError("max_workers must be an integer >= 1")
        self._max_workers = max_workers
        self._name = name
        self._logger = logger if logger is not None else logging.getLogger(name)
        self._lock = threading.RLock()
        self._entries: list[_Entry] = []
        self._cache: WeakKeyDictionary[type[Any], tuple[Handler, ...]] = WeakKeyDictionary()
        self._closed = False

    def subscribe(self, event_type: type[T], handler: Callable[[T], None]) -> Subscription:
        """Register ``handler`` for ``event_type`` and return its subscription."""
        if not isinstance(event_type, type):
            raise TypeError("event_type must be a class")
        if not callable(handler):
            raise TypeError("handler must be callable")
        typed_handler = cast(Handler, handler)
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot subscribe on a closed bus")
            subscription = Subscription(self, event_type, typed_handler)
            self._entries.append(_Entry(event_type, typed_handler, subscription))
            self._cache.clear()
        self._logger.debug("Subscribed %r to %r", typed_handler, event_type)
        return subscription

    def publish(self, event: object) -> None:
        """Deliver ``event`` synchronously to every matching handler."""
        for handler in self._snapshot(type(event)):
            handler(event)

    def _snapshot(self, event_type: type[Any]) -> tuple[Handler, ...]:
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot publish on a closed bus")
            handlers = self._cache.get(event_type)
            if handlers is None:
                handlers = tuple(
                    entry.handler
                    for entry in self._entries
                    if issubclass(event_type, entry.event_type)
                )
                self._cache[event_type] = handlers
            return handlers

    def _cancel(self, subscription: Subscription) -> None:
        with self._lock:
            if not subscription._active:
                return
            subscription._active = False
            self._entries = [
                entry for entry in self._entries if entry.subscription is not subscription
            ]
            self._cache.clear()
        self._logger.debug("Cancelled subscription %r", subscription)
