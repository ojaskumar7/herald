from __future__ import annotations

from herald import EventBus


class BaseEvent:
    pass


class ChildEvent(BaseEvent):
    pass


class GrandChildEvent(ChildEvent):
    pass


class OtherEvent:
    pass


def test_exact_type_match() -> None:
    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(BaseEvent, calls.append)

    event = BaseEvent()
    bus.publish(event)

    assert calls == [event]


def test_subclass_receives_base_handler() -> None:
    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(BaseEvent, calls.append)

    event = ChildEvent()
    bus.publish(event)

    assert calls == [event]


def test_grandchild_receives_all_ancestor_handlers() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(BaseEvent, lambda event: calls.append("base"))
    bus.subscribe(ChildEvent, lambda event: calls.append("child"))

    bus.publish(GrandChildEvent())

    assert calls == ["base", "child"]


def test_unrelated_type_does_not_match() -> None:
    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(BaseEvent, calls.append)

    bus.publish(OtherEvent())

    assert calls == []


def test_matching_handlers_run_in_global_registration_order() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(ChildEvent, lambda event: calls.append("child-first"))
    bus.subscribe(BaseEvent, lambda event: calls.append("base-second"))
    bus.subscribe(ChildEvent, lambda event: calls.append("child-third"))

    bus.publish(GrandChildEvent())

    assert calls == ["child-first", "base-second", "child-third"]


def test_subscribing_invalidates_cached_handler_list() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(BaseEvent, lambda event: calls.append("base"))
    bus.publish(ChildEvent())

    bus.subscribe(ChildEvent, lambda event: calls.append("child"))
    bus.publish(ChildEvent())

    assert calls == ["base", "base", "child"]


def test_no_handler_registered_for_unrelated_subclass() -> None:
    calls: list[object] = []
    bus = EventBus()
    bus.subscribe(OtherEvent, calls.append)

    bus.publish(GrandChildEvent())

    assert calls == []
