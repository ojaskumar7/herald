from __future__ import annotations

import gc
import threading
import weakref

from herald import EventBus, subscribe


class Ping:
    pass


class Pong:
    pass


def test_parallel_publish_with_subscription_churn() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: None)
    errors: list[BaseException] = []
    start = threading.Barrier(7, timeout=5)

    def publisher() -> None:
        try:
            start.wait()
            for _ in range(300):
                bus.publish(Ping())
        except BaseException as exc:
            errors.append(exc)

    def churner() -> None:
        try:
            start.wait()
            for _ in range(150):
                subscription = bus.subscribe(Ping, lambda event: None)
                subscription.cancel()
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=publisher),
        threading.Thread(target=publisher),
        threading.Thread(target=publisher),
        threading.Thread(target=publisher),
        threading.Thread(target=churner),
        threading.Thread(target=churner),
        threading.Thread(target=churner),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)


def test_parallel_register_and_unregister() -> None:
    class Listener:
        @subscribe
        def on_ping(self, event: Ping) -> None:
            pass

    bus = EventBus()
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            for _ in range(50):
                listener = Listener()
                bus.register(listener)
                bus.publish(Ping())
                bus.unregister(listener)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)


def test_reentrant_publish_from_handler() -> None:
    calls: list[str] = []
    bus = EventBus()

    def on_ping(event: Ping) -> None:
        calls.append("ping")
        bus.publish(Pong())

    def on_pong(event: Pong) -> None:
        calls.append("pong")

    bus.subscribe(Ping, on_ping)
    bus.subscribe(Pong, on_pong)

    bus.publish(Ping())

    assert calls == ["ping", "pong"]


def test_subscription_added_during_dispatch_does_not_receive_in_flight_event() -> None:
    added: list[Ping] = []
    bus = EventBus()

    def first(event: Ping) -> None:
        bus.subscribe(Ping, added.append)

    bus.subscribe(Ping, first)
    in_flight = Ping()
    bus.publish(in_flight)

    assert added == []

    later = Ping()
    bus.publish(later)

    assert added == [later]


def test_subscription_cancelled_during_dispatch_may_still_run() -> None:
    calls: list[str] = []
    holder: dict[str, object] = {}
    bus = EventBus()

    def first(event: Ping) -> None:
        calls.append("first")
        holder["second"].cancel()  # type: ignore[union-attr]

    def second(event: Ping) -> None:
        calls.append("second")

    bus.subscribe(Ping, first)
    holder["second"] = bus.subscribe(Ping, second)

    bus.publish(Ping())
    bus.publish(Ping())

    assert calls == ["first", "second", "first"]


def test_event_class_cache_uses_weak_references() -> None:
    class Base:
        pass

    bus = EventBus()
    bus.subscribe(Base, lambda event: None)
    event_type = type("TemporaryEvent", (Base,), {})
    ref = weakref.ref(event_type)
    bus.publish(event_type())
    del event_type
    gc.collect()

    assert ref() is None


def test_async_publishes_with_concurrent_subscription_churn() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: None)
    errors: list[BaseException] = []

    def churner() -> None:
        try:
            for _ in range(50):
                subscription = bus.subscribe(Ping, lambda event: None)
                subscription.cancel()
        except BaseException as exc:
            errors.append(exc)

    churn = threading.Thread(target=churner)
    churn.start()
    futures = [bus.publish_async(Ping()) for _ in range(100)]
    for future in futures:
        assert future.result(timeout=5) is None
    churn.join(timeout=5)

    assert not errors
    assert not churn.is_alive()
