"""A small end-to-end Herald example."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from herald import DeadEvent, EventBus, subscribe


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    total: float


@dataclass(frozen=True)
class OrderShipped:
    order_id: str


class AuditLog:
    def __init__(self) -> None:
        self.entries: list[str] = []

    @subscribe
    def on_order_placed(self, event: OrderPlaced) -> None:
        self.entries.append(f"placed {event.order_id} for {event.total:.2f}")

    @subscribe
    def on_order_shipped(self, event: OrderShipped) -> None:
        self.entries.append(f"shipped {event.order_id}")


def on_dead_event(event: DeadEvent) -> None:
    if event.exception is not None:
        print(f"delivery failed: {event.exception!r}")
    else:
        print(f"no subscriber for {event.event!r}")


def main() -> None:
    logging.basicConfig(level=logging.ERROR)

    with EventBus(name="shop") as bus:
        audit = AuditLog()
        bus.register(audit)
        bus.subscribe(DeadEvent, on_dead_event)

        bus.publish(OrderPlaced("A-1001", 42.50))
        bus.publish(OrderShipped("A-1001"))

        future = bus.publish_async(OrderPlaced("A-1002", 7.25))
        future.result()

        print("\n".join(audit.entries))

        bus.publish("not-a-known-event")


if __name__ == "__main__":
    main()
