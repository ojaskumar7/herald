# Herald

A lightweight, in-process publish/subscribe event bus for Python.

Herald routes typed events to subscribed handlers, isolates handler failures,
and reports failed or unhandled deliveries as dead letters. It has no runtime
dependencies beyond the standard library.

```python
from dataclasses import dataclass

from herald import EventBus, subscribe


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


class AuditLog:
    def __init__(self) -> None:
        self.entries: list[str] = []

    @subscribe
    def on_order(self, event: OrderPlaced) -> None:
        self.entries.append(event.order_id)


bus = EventBus()
audit = AuditLog()
bus.register(audit)

bus.publish(OrderPlaced("A-1001"))
assert audit.entries == ["A-1001"]
```

## Features

- Typed routing through the event class MRO (`OrderPlaced` reaches handlers for
  `OrderPlaced` and its base classes).
- Synchronous `publish` and executor-backed `publish_async`.
- Decorator-based discovery with `@subscribe` and `bus.register(obj)`.
- Error isolation: one failing handler never blocks the rest of the delivery.
- Dead letters for failed handlers and for events with no subscribers.
- Thread-safe subscribe, unsubscribe, register, and publish.
- Strict typing, `py.typed`, and no runtime dependencies.

## Installation

Herald is not published to PyPI yet. Install from a checkout:

```bash
python -m pip install -e ".[dev]"
```

Requires Python 3.10 or newer.

## Subscriptions

Subscribe a callable directly:

```python
from herald import EventBus

bus = EventBus()
subscription = bus.subscribe(OrderPlaced, lambda event: print(event.order_id))
subscription.cancel()
```

Or let Herald discover decorated methods:

```python
from herald import subscribe


class Metrics:
    @subscribe
    def on_order(self, event: OrderPlaced) -> None: ...

    @subscribe(OrderPlaced)
    def on_any_event(self, event: object) -> None: ...


bus.register(Metrics())
```

`register` scans the class MRO from most-derived to base and returns the
created subscriptions. `unregister(obj)` cancels them and returns the count.
Duplicate registrations accumulate, matching the rule that the same handler may
be subscribed more than once.

## Dead letters

When a handler raises, Herald logs the exception and publishes a `DeadEvent` on
the same bus. When an event has no matching subscribers, it publishes a
`DeadEvent` with no handler or exception attached.

```python
from herald import DeadEvent


def on_dead_event(event: DeadEvent) -> None:
    if event.exception is not None:
        print("delivery failed", event.exception)
    else:
        print("nobody listened to", event.event)


bus.subscribe(DeadEvent, on_dead_event)
```

`DeadEvent` is never dead-lettered again: a failing dead-letter handler is only
logged. A `DeadEvent` with no subscribers is dropped silently.

## Async dispatch

`publish_async` submits a delivery to the bus executor and returns a
`concurrent.futures.Future`:

```python
future = bus.publish_async(OrderPlaced("A-1002"))
future.result()
```

Handlers within one delivery run sequentially; separate deliveries may run
concurrently. Handler exceptions are dead-lettered and do not fail the future.
From asyncio code, use the bridge:

```python
await bus.publish_awaitable(OrderPlaced("A-1003"))
```

## Lifecycle

```python
with EventBus(max_workers=4, name="orders") as bus:
    bus.publish(OrderPlaced("A-1004"))
```

`close(wait=True)` rejects new publishes and subscriptions and waits for
in-flight async deliveries. `close(wait=False)` cancels queued deliveries.
Closing from inside a handler is safe: waiting is downgraded to a non-blocking
shutdown to avoid joining the current worker thread.

## Threading model

An `RLock` guards the registry. Dispatch takes an immutable handler snapshot
under the lock, releases it, and only then calls handlers. Handlers can
therefore subscribe, cancel, or publish re-entrantly without deadlocking. A
handler subscribed during dispatch does not see the in-flight event; a handler
already in the snapshot may still run after being cancelled. Resolved handler
lists are cached per event class in a weak-keyed dictionary and invalidated on
every registry mutation.

## Development

```bash
.venv/bin/pytest --cov=herald
.venv/bin/mypy
.venv/bin/ruff check .
.venv/bin/ruff format --check .
```

See [`SPEC.md`](SPEC.md) for the behavioral contract and
[`docs/technical-blog.md`](docs/technical-blog.md) for a deeper design write-up.

## License

MIT.
