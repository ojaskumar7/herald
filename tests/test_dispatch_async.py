from __future__ import annotations

import threading
import time
from concurrent.futures import Future

import pytest

from herald import BusClosedError, DeadEvent, EventBus


class Ping:
    pass


def test_publish_async_returns_future_and_completes() -> None:
    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(Ping, calls.append)
    event = Ping()

    future = bus.publish_async(event)

    assert isinstance(future, Future)
    assert future.result(timeout=2) is None
    assert calls == [event]


def test_async_delivery_runs_handlers_in_order() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(Ping, lambda event: calls.append("first"))
    bus.subscribe(Ping, lambda event: calls.append("second"))

    bus.publish_async(Ping()).result(timeout=2)

    assert calls == ["first", "second"]


def test_separate_async_publishes_run_concurrently() -> None:
    guard = threading.Lock()
    active = 0
    max_active = 0

    def handler(event: Ping) -> None:
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.1)
        with guard:
            active -= 1

    bus = EventBus()
    bus.subscribe(Ping, handler)

    futures = [bus.publish_async(Ping()) for _ in range(4)]
    for future in futures:
        future.result(timeout=2)

    assert max_active >= 2


def test_failing_handler_does_not_fail_async_future() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(RuntimeError("boom")))

    future = bus.publish_async(Ping())

    assert future.result(timeout=2) is None


def test_base_exception_is_recorded_on_async_future() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(KeyboardInterrupt()))

    future = bus.publish_async(Ping())

    with pytest.raises(KeyboardInterrupt):
        future.result(timeout=2)


def test_async_dead_letter_is_delivered_on_worker_thread() -> None:
    handler_threads: list[int] = []
    dead_threads: list[int] = []

    def failing(event: Ping) -> None:
        handler_threads.append(threading.get_ident())
        raise RuntimeError("boom")

    def dead(event: DeadEvent) -> None:
        dead_threads.append(threading.get_ident())

    bus = EventBus()
    bus.subscribe(Ping, failing)
    bus.subscribe(DeadEvent, dead)

    bus.publish_async(Ping()).result(timeout=2)

    assert handler_threads == dead_threads
    assert handler_threads[0] != threading.get_ident()


def test_async_event_without_subscribers_produces_dead_event() -> None:
    received: list[DeadEvent] = []
    bus = EventBus()
    bus.subscribe(DeadEvent, received.append)

    bus.publish_async(Ping()).result(timeout=2)

    assert len(received) == 1
    assert isinstance(received[0].event, Ping)


def test_publish_async_after_close_raises() -> None:
    bus = EventBus()
    bus.close()

    with pytest.raises(BusClosedError):
        bus.publish_async(Ping())


def test_publish_awaitable_completes() -> None:
    import asyncio

    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(Ping, calls.append)

    async def run() -> None:
        await bus.publish_awaitable(Ping())

    asyncio.run(run())

    assert len(calls) == 1
