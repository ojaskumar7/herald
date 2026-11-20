from __future__ import annotations

import logging

import pytest

from herald import DeadEvent, EventBus


class Ping:
    pass


class Pong:
    pass


def test_failed_handler_produces_dead_event_with_details() -> None:
    received: list[DeadEvent] = []
    bus = EventBus()

    def failing(event: Ping) -> None:
        raise ValueError("bad event")

    bus.subscribe(Ping, failing)
    bus.subscribe(DeadEvent, received.append)
    event = Ping()

    bus.publish(event)

    assert len(received) == 1
    dead = received[0]
    assert dead.event is event
    assert dead.handler is failing
    assert isinstance(dead.exception, ValueError)


def test_event_without_subscribers_produces_dead_event() -> None:
    received: list[DeadEvent] = []
    bus = EventBus()
    bus.subscribe(DeadEvent, received.append)
    event = Ping()

    bus.publish(event)

    assert len(received) == 1
    assert received[0].event is event
    assert received[0].handler is None
    assert received[0].exception is None


def test_dead_event_without_subscribers_is_dropped_silently() -> None:
    bus = EventBus()
    bus.publish(DeadEvent(Ping()))


def test_failing_dead_event_handler_is_not_dead_lettered_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    bus = EventBus()

    def failing_dead_handler(event: DeadEvent) -> None:
        raise RuntimeError("cannot handle dead letter")

    bus.subscribe(DeadEvent, failing_dead_handler)

    with caplog.at_level(logging.ERROR, logger="herald"):
        bus.publish(Ping())

    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "DeadEvent handler" in errors[0].getMessage()
    assert errors[0].exc_info is not None


def test_dead_event_handler_failure_does_not_loop() -> None:
    calls: list[DeadEvent] = []
    bus = EventBus()

    def failing_dead_handler(event: DeadEvent) -> None:
        calls.append(event)
        raise RuntimeError("cannot handle dead letter")

    bus.subscribe(DeadEvent, failing_dead_handler)

    bus.publish(Ping())

    assert len(calls) == 1


def test_no_dead_letter_for_successful_delivery() -> None:
    dead: list[DeadEvent] = []
    bus = EventBus()
    bus.subscribe(Ping, lambda event: None)
    bus.subscribe(DeadEvent, dead.append)

    bus.publish(Ping())

    assert dead == []


def test_dead_letter_delivered_for_matching_subclass() -> None:
    class ChildEvent(Ping):
        pass

    received: list[DeadEvent] = []
    bus = EventBus()
    bus.subscribe(DeadEvent, received.append)

    bus.publish(ChildEvent())

    assert len(received) == 1
    assert isinstance(received[0].event, ChildEvent)
