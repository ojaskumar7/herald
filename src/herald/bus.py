"""The Herald event bus."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, TypeVar, cast
from weakref import WeakKeyDictionary

from .dead_letter import DeadEvent
from .errors import BusClosedError
from .subscriber import collect_subscribers

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
        self._owners: dict[int, list[Subscription]] = {}
        self._cache: WeakKeyDictionary[type[Any], tuple[Handler, ...]] = WeakKeyDictionary()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
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

    def register(self, obj: object) -> list[Subscription]:
        """Register every ``@subscribe`` method on ``obj``."""
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot register on a closed bus")
        discovered = collect_subscribers(obj)
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot register on a closed bus")
            subscriptions: list[Subscription] = []
            for event_type, handler in discovered:
                typed_handler = cast(Handler, handler)
                subscription = Subscription(self, event_type, typed_handler)
                self._entries.append(_Entry(event_type, typed_handler, subscription))
                subscriptions.append(subscription)
            self._owners.setdefault(id(obj), []).extend(subscriptions)
            self._cache.clear()
        self._logger.debug("Registered %d subscription(s) for %r", len(subscriptions), obj)
        return subscriptions

    def unregister(self, obj: object) -> int:
        """Cancel every subscription created by ``register(obj)``."""
        with self._lock:
            subscriptions = self._owners.pop(id(obj), [])
            if not subscriptions:
                return 0
            cancelled = {id(subscription) for subscription in subscriptions if subscription._active}
            for subscription in subscriptions:
                subscription._active = False
            self._entries = [
                entry for entry in self._entries if id(entry.subscription) not in cancelled
            ]
            self._cache.clear()
        self._logger.debug("Unregistered %d subscription(s) from %r", len(cancelled), obj)
        return len(cancelled)

    def publish(self, event: object) -> None:
        """Deliver ``event`` synchronously to every matching handler."""
        self._deliver(event)

    def publish_async(self, event: object) -> Future[None]:
        """Submit ``event`` for delivery on the bus executor."""
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot publish on a closed bus")
            try:
                return self._executor.submit(self._deliver, event)
            except RuntimeError as exc:
                raise BusClosedError("cannot publish on a closed bus") from exc

    async def publish_awaitable(self, event: object) -> None:
        """Deliver ``event`` on the executor and await its completion."""
        import asyncio

        future = self.publish_async(event)
        await asyncio.wrap_future(future)

    def _deliver(self, event: object) -> None:
        handlers = self._snapshot(type(event))
        if not handlers:
            if isinstance(event, DeadEvent):
                self._logger.debug("Dropping dead letter for %r", event.event)
                return
            self._emit_dead_letter(DeadEvent(event))
            return
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                self._handle_failure(event, handler, exc)

    def _handle_failure(self, event: object, handler: Handler, exc: Exception) -> None:
        if isinstance(event, DeadEvent):
            self._logger.error(
                "DeadEvent handler %r failed for %r", handler, event.event, exc_info=exc
            )
            return
        self._logger.error("Handler %r failed for %r", handler, event, exc_info=exc)
        self._emit_dead_letter(DeadEvent(event, handler, exc))

    def _emit_dead_letter(self, dead_event: DeadEvent) -> None:
        self._logger.debug("Publishing dead letter for %r", dead_event.event)
        try:
            self.publish(dead_event)
        except BusClosedError:
            self._logger.debug("Bus closed while publishing dead letter")

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

    def close(self, *, wait: bool = True) -> None:
        """Close the bus, optionally waiting for in-flight async deliveries."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            executor = self._executor
            if wait and self._is_worker_thread():
                self._logger.warning(
                    "close(wait=True) called from a bus worker thread; using wait=False"
                )
                wait = False
        executor.shutdown(wait=wait, cancel_futures=not wait)
        self._logger.debug("Closed event bus %r", self._name)

    def _is_worker_thread(self) -> bool:
        return threading.current_thread().name.startswith(f"{self._name}_")

    def __enter__(self) -> EventBus:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
