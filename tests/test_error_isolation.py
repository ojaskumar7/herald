from __future__ import annotations

import logging

import pytest

from herald import EventBus


class Ping:
    pass


class Pong:
    pass


def test_failing_handler_does_not_block_later_handlers() -> None:
    calls: list[str] = []
    bus = EventBus()

    def failing(event: Ping) -> None:
        calls.append("failing")
        raise RuntimeError("boom")

    bus.subscribe(Ping, failing)
    bus.subscribe(Ping, lambda event: calls.append("after"))

    bus.publish(Ping())

    assert calls == ["failing", "after"]


def test_failing_handler_is_logged_with_traceback(caplog: pytest.LogCaptureFixture) -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(RuntimeError("boom")))

    with caplog.at_level(logging.ERROR, logger="herald"):
        bus.publish(Ping())

    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None


def test_keyboard_interrupt_propagates() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(KeyboardInterrupt()))

    with pytest.raises(KeyboardInterrupt):
        bus.publish(Ping())


def test_system_exit_propagates() -> None:
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(SystemExit(3)))

    with pytest.raises(SystemExit):
        bus.publish(Ping())


def test_base_exception_stops_later_handlers() -> None:
    calls: list[str] = []
    bus = EventBus()

    def interrupt(event: Ping) -> None:
        raise KeyboardInterrupt

    bus.subscribe(Ping, interrupt)
    bus.subscribe(Ping, lambda event: calls.append("after"))

    with pytest.raises(KeyboardInterrupt):
        bus.publish(Ping())

    assert calls == []


def test_exception_from_one_event_does_not_affect_other_events() -> None:
    calls: list[str] = []
    bus = EventBus()
    bus.subscribe(Ping, lambda event: (_ for _ in ()).throw(RuntimeError("boom")))
    bus.subscribe(Pong, lambda event: calls.append("pong"))

    bus.publish(Ping())
    bus.publish(Pong())

    assert calls == ["pong"]
