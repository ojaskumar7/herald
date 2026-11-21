from __future__ import annotations

import threading
import time

import pytest

from herald import BusClosedError, EventBus


class Ping:
    pass


def test_context_manager_closes_bus() -> None:
    with EventBus() as bus:
        bus.subscribe(Ping, lambda event: None)

    with pytest.raises(BusClosedError):
        bus.publish(Ping())


def test_close_is_idempotent() -> None:
    bus = EventBus()
    bus.close()
    bus.close()
    bus.close(wait=False)


def test_close_wait_false_then_wait_true_is_noop() -> None:
    bus = EventBus()
    bus.close(wait=False)
    bus.close(wait=True)


def test_subscribe_after_close_raises() -> None:
    bus = EventBus()
    bus.close()

    with pytest.raises(BusClosedError):
        bus.subscribe(Ping, lambda event: None)


def test_register_after_close_raises() -> None:
    bus = EventBus()
    bus.close()

    with pytest.raises(BusClosedError):
        bus.register(object())


def test_unregister_after_close_is_allowed() -> None:
    bus = EventBus()
    listener = object()
    bus.close()

    assert bus.unregister(listener) == 0


def test_subscription_cancel_after_close_is_noop() -> None:
    bus = EventBus()
    subscription = bus.subscribe(Ping, lambda event: None)
    bus.close()

    subscription.cancel()
    subscription.cancel()

    assert subscription.active is False


def test_close_waits_for_inflight_async_delivery() -> None:
    started = threading.Event()
    finished = threading.Event()

    def slow(event: Ping) -> None:
        started.set()
        time.sleep(0.1)
        finished.set()

    bus = EventBus()
    bus.subscribe(Ping, slow)
    future = bus.publish_async(Ping())

    assert started.wait(timeout=2)
    bus.close()

    assert finished.is_set()
    assert future.done()


def test_close_wait_false_cancels_queued_deliveries() -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking(event: Ping) -> None:
        started.set()
        release.wait(timeout=2)

    bus = EventBus(max_workers=1)
    bus.subscribe(Ping, blocking)
    running = bus.publish_async(Ping())
    assert started.wait(timeout=2)
    queued = bus.publish_async(Ping())

    bus.close(wait=False)

    assert queued.cancelled()
    release.set()
    assert running.result(timeout=2) is None


def test_close_from_worker_thread_does_not_deadlock() -> None:
    completed = threading.Event()
    bus = EventBus()

    def handler(event: Ping) -> None:
        bus.close()
        completed.set()

    bus.subscribe(Ping, handler)
    future = bus.publish_async(Ping())

    assert completed.wait(timeout=2)
    assert future.result(timeout=2) is None
    with pytest.raises(BusClosedError):
        bus.publish(Ping())


def test_close_from_sync_handler_does_not_deadlock() -> None:
    bus = EventBus()

    def handler(event: Ping) -> None:
        bus.close()

    bus.subscribe(Ping, handler)
    bus.publish(Ping())

    with pytest.raises(BusClosedError):
        bus.publish(Ping())
