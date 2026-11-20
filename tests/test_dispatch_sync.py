from __future__ import annotations

from herald import EventBus


class Ping:
    pass


def test_handlers_run_in_registration_order() -> None:
    calls: list[str] = []
    bus = EventBus()

    bus.subscribe(Ping, lambda event: calls.append("first"))
    bus.subscribe(Ping, lambda event: calls.append("second"))
    bus.subscribe(Ping, lambda event: calls.append("third"))

    bus.publish(Ping())

    assert calls == ["first", "second", "third"]


def test_same_handler_subscribed_twice_runs_twice() -> None:
    calls: list[object] = []

    def handler(event: object) -> None:
        calls.append(event)

    bus = EventBus()
    bus.subscribe(Ping, handler)
    bus.subscribe(Ping, handler)

    event = Ping()
    bus.publish(event)

    assert calls == [event, event]


def test_publish_returns_after_all_handlers_run() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(Ping, lambda event: calls.append("handler"))

    result = bus.publish(Ping())

    assert result is None
    assert calls == ["handler"]


def test_publish_without_subscribers_does_not_raise() -> None:
    bus = EventBus()
    bus.publish(Ping())


def test_multiple_event_types_keep_independent_handler_sets() -> None:
    calls: list[str] = []

    class Pong:
        pass

    bus = EventBus()
    bus.subscribe(Ping, lambda event: calls.append("ping"))
    bus.subscribe(Pong, lambda event: calls.append("pong"))

    bus.publish(Ping())

    assert calls == ["ping"]
