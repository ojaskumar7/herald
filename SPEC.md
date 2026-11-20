# Herald - Project Specification

**Version:** 0.1.0 (draft)
**Status:** Proposed

---

## 1. Overview

Herald is a lightweight, in-process publish/subscribe event bus for Python. Publishers emit typed events without knowing who listens, and subscribers react without knowing who published. Herald provides synchronous and asynchronous dispatch, dead-letter handling for failed or unhandled events, and decorator-based subscriber auto-discovery.

### 1.1 Goals
- Decouple application components through typed events.
- Type safety: full type hints, `mypy --strict` clean.
- Thread safety: safe concurrent subscribe, unsubscribe, and publish.
- Error isolation: one failing handler never affects others or the publisher.
- Zero runtime dependencies (standard library only).

### 1.2 Non-Goals
- Cross-process or networked delivery.
- Event persistence, replay, or guaranteed delivery.
- Handler priorities or ordering beyond registration order.
- Automatic retries.

### 1.3 Requirements
- Python 3.10 or newer.
- Dev tooling: pytest, mypy (strict), ruff.

---

## 2. Concepts

| Term | Meaning |
|---|---|
| **Event** | Any Python object. Its class determines routing. |
| **Handler** | A callable taking one event argument and returning nothing. |
| **Subscription** | A handle for one handler registration, with `cancel()`. |
| **Dead event** | A `DeadEvent` wrapper published when delivery fails or nobody listens. |

---

## 3. Architecture

### Package layout

```
herald/
├── pyproject.toml
├── README.md
├── SPEC.md
├── examples/
│   └── basic_usage.py
├── src/herald/
│   ├── __init__.py       # public API exports
│   ├── bus.py            # EventBus
│   ├── subscriber.py     # @subscribe decorator, scanning
│   ├── dead_letter.py    # DeadEvent
│   ├── errors.py         # exceptions
│   └── py.typed          # PEP 561 marker
└── tests/
    ├── test_dispatch_sync.py
    ├── test_dispatch_async.py
    ├── test_routing.py
    ├── test_error_isolation.py
    ├── test_dead_letters.py
    ├── test_register.py
    └── test_concurrency.py
```

---

## 4. Public API

### 4.1 `EventBus`

```python
class EventBus:
    def __init__(
        self,
        *,
        max_workers: int = 4,
        name: str = "herald",
        logger: logging.Logger | None = None,
    ) -> None: ...

    # Subscription
    def subscribe(self, event_type: type[T], handler: Callable[[T], None]) -> Subscription: ...
    def register(self, obj: object) -> list[Subscription]: ...
    def unregister(self, obj: object) -> int: ...

    # Publishing
    def publish(self, event: object) -> None: ...
    def publish_async(self, event: object) -> Future[None]: ...

    # Lifecycle
    def close(self, *, wait: bool = True) -> None: ...
    def __enter__(self) -> EventBus: ...
    def __exit__(self, *exc: object) -> None: ...
```

- `name` is used as the executor thread-name prefix and as the logger name when `logger` is not supplied. Default `"herald"`.
- `logger` overrides the logger derived from `name`. The library never adds handlers or changes global configuration.
- `max_workers` must be an integer `>= 1`; otherwise `ValueError` is raised in `__init__`.
- `publish_async` returns a `concurrent.futures.Future[None]`, never an `asyncio.Future`.

### 4.2 `Subscription`

```python
class Subscription:
    @property
    def event_type(self) -> type: ...
    @property
    def active(self) -> bool: ...
    def cancel(self) -> None: ...   # idempotent
```

- `event_type` and `active` are read-only properties.
- `active` becomes `False` after `cancel()` or after the owning bus cancels the subscription via `unregister`.
- `cancel()` after an earlier `cancel()`, or after the bus is closed, is a no-op.

### 4.3 `@subscribe`

```python
@subscribe                    # event type inferred from first parameter's type hint
@subscribe(EventType)         # explicit override
```

Marks a method as a handler. It takes effect only when its owner is passed to `bus.register(obj)`.

### 4.4 `DeadEvent`

```python
@dataclass(frozen=True)
class DeadEvent:
    event: object                      # the original event
    handler: Callable[..., None] | None  # failing handler; None if no subscribers
    exception: BaseException | None      # raised exception; None if no subscribers
```

### 4.5 Exceptions

| Exception | Raised when |
|---|---|
| `HeraldError` | Base class for all library errors; subclasses `Exception`. |
| `BusClosedError` | Publishing, subscribing, or registering on a closed bus. |
| `SubscriberDefinitionError` | A `@subscribe` method has an invalid signature, a missing type hint with no override, or the wrong arity. Raised at `register` time. |

- `BusClosedError` subclasses both `HeraldError` and `RuntimeError`.
- `SubscriberDefinitionError` subclasses both `HeraldError` and `TypeError`.

---

## 5. Behavior Specification

### 5.1 Subscription
- `subscribe` returns a `Subscription`. The same handler may be subscribed multiple times and is then called once per subscription.
- `handler` must be callable; a non-callable argument raises `TypeError`. `subscribe` does not validate the handler's arity; a handler with the wrong arity fails at dispatch time and is dead-lettered like any other handler exception.
- Calling `subscribe` or `register` on a closed bus raises `BusClosedError`.
- `register(obj)` scans `obj`'s class for `@subscribe` methods (including inherited ones), binds them to the instance, and subscribes each. It returns the created subscriptions.
- Repeated `register(obj)` calls create fresh subscriptions that accumulate; the bus does not deduplicate by handler or object.
- `unregister(obj)` cancels every subscription created by any `register(obj)` call for that instance, including earlier duplicate registrations, and returns the total count cancelled. It is a no-op for unknown objects and remains permitted after the bus is closed.
- Scanning order is deterministic: the class MRO from most-derived to base, and within each class the declaration order from `vars(cls)`. If a method name is overridden in a subclass, only the most-derived definition is registered. Methods that are not decorated with `@subscribe` are ignored, including overrides that remove the decorator.
- Event type resolution for `@subscribe`: the explicit argument wins, otherwise the first non-`self` parameter's type hint. Failure raises `SubscriberDefinitionError` at `register` time. This includes a missing annotation, no non-`self` parameter, or more than one non-`self` parameter.

### 5.2 Routing
- A handler for type `T` receives events whose class is `T` or a subclass of `T`, resolved through the event class's MRO.
- Handlers run in registration order: the global order in which subscriptions acquired the bus lock, across all matching event types.
- Resolved handler lists are cached per event class in a `WeakKeyDictionary`, so cached classes remain garbage-collectable. Any subscribe or unsubscribe clears the cache.
- Cache access and invalidation happen under the bus lock; dispatch uses the immutable snapshot described in 5.7.

### 5.3 Synchronous dispatch (`publish`)
- Runs matching handlers in the caller's thread, in order, and returns after all have run.
- Never raises because of handler failures (see 5.5).
- Dead letters generated during delivery are published synchronously on the same thread.
- Raises `BusClosedError` if the bus is closed.

### 5.4 Asynchronous dispatch (`publish_async`)
- Submits delivery to a `ThreadPoolExecutor` and returns a `concurrent.futures.Future[None]` immediately.
- Within one delivery, handlers run sequentially in order. Separate async publishes may run concurrently.
- The future completes normally when handler exceptions (`Exception`) occur; those failures are dead-lettered, not propagated through the future.
- `KeyboardInterrupt` and `SystemExit` are not swallowed: the standard executor records them on the future via `set_exception` and re-raises them in the worker thread. The future does not complete normally in that case.
- Dead letters generated by an async delivery are published synchronously on the worker thread through `publish`, never through `publish_async`. This avoids pool recursion and self-deadlock when the pool is saturated.
- Raises `BusClosedError` synchronously if the bus is closed. If the executor has already begun shutting down, the failed submission is translated to `BusClosedError`.
- `close(wait=False)` may cancel deliveries that have been submitted but not started; their futures report `cancelled() == True`.

### 5.5 Error isolation
- Each handler call is wrapped individually. An exception is caught (`Exception`, not `BaseException`), logged through `logging` at `ERROR` with the traceback, and converted to a `DeadEvent`. Remaining handlers still run.
- `KeyboardInterrupt` and `SystemExit` are not swallowed. In `publish` they propagate out of the call; in `publish_async` they are recorded on the future and re-raised in the worker thread.
- No retries.

### 5.6 Dead letters
A `DeadEvent` is published on the same bus, synchronously on the delivering thread, when:
1. A handler raises an `Exception` (`handler` and `exception` populated).
2. An event has no matching subscribers (`handler` and `exception` are `None`).

Loop prevention:
- `DeadEvent` itself is never dead-lettered. A failing `DeadEvent` handler is only logged at `ERROR` with the traceback, and no further `DeadEvent` is emitted.
- A `DeadEvent` with no subscribers is dropped silently (a `DEBUG` log entry may still be emitted).

### 5.7 Thread safety
- An `RLock` guards registry mutations (subscribe, unsubscribe, register, unregister, cache invalidation), subscription `active` state, and the closed flag.
- Dispatch takes an immutable snapshot of the handler list under the lock, then releases it before invoking handlers. Handlers may therefore call `subscribe`, `unsubscribe`, or `publish` without deadlock.
- A handler subscribed during dispatch does not receive the in-flight event. A handler cancelled during dispatch may still receive the in-flight event if it is already in the snapshot.
- Registration order is defined by the order in which concurrent `subscribe` calls acquire the lock.

### 5.8 Lifecycle
- `close(wait=True)` marks the bus closed, rejects new publishes, subscribes, and registers, and shuts down the executor, waiting for in-flight async deliveries to finish.
- `close(wait=False)` marks the bus closed and performs a non-blocking shutdown with `cancel_futures=True`: queued deliveries are cancelled and running deliveries continue in the background without being awaited. Their futures are not retroactively cancelled.
- `close()` is safe to call from inside a handler. When `close(wait=True)` is invoked from one of the bus's own executor worker threads, waiting would deadlock; the implementation performs a non-blocking shutdown instead, logs at `WARNING`, and returns with the bus closed.
- `close()` is idempotent; subsequent calls are no-ops regardless of `wait`.
- The context manager calls `close(wait=True)` on exit and does not suppress exceptions raised in the `with` body.
- `unregister` and `Subscription.cancel()` remain allowed after close.

### 5.9 Optional asyncio support
- Core `EventBus` stays asyncio-free. `publish_async` returns a `concurrent.futures.Future`.
- The opt-in bridge is `async def publish_awaitable(self, event: object) -> None:` which delivers through the executor and awaits the resulting future with `asyncio.wrap_future`, so the event loop is never blocked. It follows the same isolation and dead-letter rules as `publish_async`.
- Coroutine handlers are supported only through the optional `AsyncEventBus` adapter. Its exact surface (constructor, handler detection, loop binding) is finalized in the asyncio milestone and does not alter sections 5.1-5.8.

---

## 6. Type Safety
- Full annotations on all public and internal code. Ship `py.typed`.
- `subscribe` is generic over the event type, so a handler's parameter type is checked against `event_type`.
- Target: `mypy --strict` passes with no errors on `src/`.

---

## 7. Logging
- Logger name defaults to `herald` and is derived from `name` unless `logger` is supplied.
- `ERROR`: handler exceptions (with traceback), failing `DeadEvent` handlers.
- `WARNING`: `close(wait=True)` forced to non-blocking shutdown because it was called from a bus worker thread.
- `DEBUG`: subscribe, unsubscribe, publish, and dead-letter routing.
- The library adds no handlers or configuration, following standard library-logging etiquette.

---

## 8. Packaging
- `pyproject.toml` with `src/` layout, using the `hatchling` build backend.
- Project name `herald` (the PyPI name may need to change if it is taken).
- Version `0.1.0`. License: MIT (owner may override before release).
- Dev extras: `pytest`, `pytest-cov`, `mypy`, `ruff`.

---

## 9. Testing Strategy

| Area | Cases |
|---|---|
| Sync dispatch | Order, multiple handlers, same handler subscribed twice |
| Async dispatch | Future completion, concurrency, publish after close, `close(wait=False)` cancels queued deliveries, `BaseException` recorded on the future |
| Routing | Exact type, subclass, base class, unrelated type, cache invalidation, weak cache does not retain classes, inherited definition order and overrides |
| Error isolation | Failing handler does not block others; logged; `KeyboardInterrupt` propagates |
| Dead letters | Failure case, no-subscriber case, loop guard, `DeadEvent` handler failure, sync delivery from `publish_async` |
| Register | Inferred hint, explicit override, inheritance, invalid definitions, duplicate register accumulates, unregister cancels all, unregister after close |
| Concurrency | Parallel publish plus subscribe/unsubscribe stress, re-entrant publish from handler, close while publishing |
| Lifecycle | Idempotent close, context manager, `BusClosedError`, `close(wait=True)` from a worker thread does not deadlock |

Coverage target: 90% or more of lines.

---

## 10. Assumptions and Open Questions

**Assumptions**
- No handler priorities.
- Handler return values are ignored.
- Single-process only.
- Strong references to handlers (no weak refs); callers must `unregister` or `cancel` to release them.
- Dead letters are always delivered synchronously, including when triggered by an async delivery.
- MIT is the default license; the owner may change it before release.

**Open questions**
- Whether the PyPI name `herald` is available (distribution name is provisional).
- Final public surface of the `AsyncEventBus` coroutine-handler adapter.

---

## 11. Milestones

1. Scaffold: `pyproject.toml`, package skeleton, tooling config.
2. Core: `EventBus` with subscribe, sync publish, and MRO routing.
3. Error isolation, `DeadEvent`, and loop guards.
4. `@subscribe` with `register` and `unregister`.
5. Async dispatch and lifecycle.
6. Test suite and `mypy --strict` cleanup.
7. README, examples, and the optional asyncio add-on.
