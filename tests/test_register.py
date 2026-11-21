from __future__ import annotations

import pytest

from herald import EventBus, SubscriberDefinitionError, subscribe


class Ping:
    pass


class Pong:
    pass


class Greeting:
    def __init__(self) -> None:
        self.received: list[Ping] = []

    @subscribe
    def on_ping(self, event: Ping) -> None:
        self.received.append(event)


def test_inferred_event_type_from_hint() -> None:
    listener = Greeting()
    bus = EventBus()

    subscriptions = bus.register(listener)
    event = Ping()
    bus.publish(event)

    assert len(subscriptions) == 1
    assert subscriptions[0].event_type is Ping
    assert listener.received == [event]


def test_explicit_event_type_overrides_hint() -> None:
    class Listener:
        def __init__(self) -> None:
            self.received: list[Pong] = []

        @subscribe(Pong)
        def on_event(self, event: object) -> None:
            self.received.append(event)

    listener = Listener()
    bus = EventBus()
    bus.register(listener)

    event = Pong()
    bus.publish(Ping())
    bus.publish(event)

    assert listener.received == [event]


def test_inherited_subscribers_are_registered() -> None:
    class Child(Greeting):
        pass

    listener = Child()
    bus = EventBus()
    bus.register(listener)

    bus.publish(Ping())

    assert len(listener.received) == 1


def test_subclass_override_without_decorator_removes_subscription() -> None:
    class Child(Greeting):
        def on_ping(self, event: Ping) -> None:
            self.received.append(event)

    listener = Child()
    bus = EventBus()
    subscriptions = bus.register(listener)
    bus.publish(Ping())

    assert subscriptions == []
    assert listener.received == []


def test_subclass_override_with_decorator_replaces_base() -> None:
    calls: list[str] = []

    class Base:
        @subscribe
        def on_ping(self, event: Ping) -> None:
            calls.append("base")

    class Child(Base):
        @subscribe
        def on_ping(self, event: Ping) -> None:
            calls.append("child")

    bus = EventBus()
    bus.register(Child())
    bus.publish(Ping())

    assert calls == ["child"]


def test_scan_order_is_mro_then_definition_order() -> None:
    calls: list[str] = []

    class Base:
        @subscribe
        def base_first(self, event: Ping) -> None:
            calls.append("base-first")

        @subscribe
        def base_second(self, event: Ping) -> None:
            calls.append("base-second")

    class Child(Base):
        @subscribe
        def child_first(self, event: Ping) -> None:
            calls.append("child-first")

        @subscribe
        def child_second(self, event: Ping) -> None:
            calls.append("child-second")

    bus = EventBus()
    bus.register(Child())
    bus.publish(Ping())

    assert calls == ["child-first", "child-second", "base-first", "base-second"]


def test_invalid_missing_hint_raises_at_register_time() -> None:
    class Listener:
        @subscribe
        def on_ping(self, event) -> None:  # type: ignore[no-untyped-def]
            pass

    bus = EventBus()

    with pytest.raises(SubscriberDefinitionError):
        bus.register(Listener())


def test_invalid_no_parameters_raises_at_register_time() -> None:
    class Listener:
        @subscribe
        def on_ping(self) -> None:
            pass

    bus = EventBus()

    with pytest.raises(SubscriberDefinitionError):
        bus.register(Listener())


def test_invalid_two_parameters_raises_at_register_time() -> None:
    class Listener:
        @subscribe
        def on_ping(self, event: Ping, extra: object) -> None:
            pass

    bus = EventBus()

    with pytest.raises(SubscriberDefinitionError):
        bus.register(Listener())


def test_invalid_varargs_raises_at_register_time() -> None:
    class Listener:
        @subscribe
        def on_ping(self, *events: Ping) -> None:
            pass

    bus = EventBus()

    with pytest.raises(SubscriberDefinitionError):
        bus.register(Listener())


def test_unregister_cancels_subscriptions_and_returns_count() -> None:
    listener = Greeting()
    bus = EventBus()
    bus.register(listener)

    count = bus.unregister(listener)
    bus.publish(Ping())

    assert count == 1
    assert listener.received == []


def test_unregister_unknown_object_is_noop() -> None:
    bus = EventBus()

    assert bus.unregister(object()) == 0


def test_duplicate_registration_accumulates() -> None:
    listener = Greeting()
    bus = EventBus()

    first = bus.register(listener)
    second = bus.register(listener)

    bus.publish(Ping())

    assert len(first) == 1
    assert len(second) == 1
    assert len(listener.received) == 2


def test_unregister_after_duplicate_registration_cancels_all() -> None:
    listener = Greeting()
    bus = EventBus()
    bus.register(listener)
    bus.register(listener)

    count = bus.unregister(listener)
    bus.publish(Ping())

    assert count == 2
    assert listener.received == []


def test_undecorated_methods_are_ignored() -> None:
    class Listener:
        def on_ping(self, event: Ping) -> None:
            raise AssertionError("should not be called")

    bus = EventBus()

    assert bus.register(Listener()) == []


def test_subscribe_requires_callable_handler() -> None:
    bus = EventBus()

    with pytest.raises(TypeError):
        bus.subscribe(Ping, object())  # type: ignore[arg-type]


def test_subscribe_requires_event_type_class() -> None:
    bus = EventBus()

    with pytest.raises(TypeError):
        bus.subscribe("Ping", lambda event: None)  # type: ignore[arg-type]
