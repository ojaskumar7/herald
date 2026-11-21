# Building Herald: A Small Event Bus with Sharp Edges Considered

Herald is a typed, in-process publish/subscribe bus for Python. It is small on
purpose: no broker, no persistence, no plugin system, and no runtime
dependencies. The interesting part is not the feature list. It is the set of
constraints that decide how a few hundred lines of Python behave when handlers
fail, threads race, and the bus is asked to shut down in the middle of a
delivery.

This is a tour of those decisions.

## The constraints

The specification set the boundaries up front:

- Python 3.10+, standard library only.
- Synchronous and executor-backed asynchronous dispatch.
- Thread-safe registration and publication.
- One failing handler must never affect the publisher or the other handlers.
- Failed and unhandled events become dead letters.
- `mypy --strict` clean, with `py.typed` shipped.

Those constraints rule out some tempting shortcuts. A global handler list
guarded by a lock during invocation is simple, but deadlocks as soon as a
handler publishes another event. A per-type dictionary is fast, but obscures
global registration order. Swallowing every `BaseException` keeps the bus
alive, but turns `KeyboardInterrupt` into a dead letter and makes shutdown
unreliable.

Herald instead uses a small number of explicit rules.

## The data model

Events are ordinary Python objects. There is no base class to inherit and no
registration macro. A handler is a callable that receives one event and returns
nothing. A `Subscription` is the handle returned by `subscribe`:

```python
subscription = bus.subscribe(OrderPlaced, on_order)
subscription.event_type  # OrderPlaced
subscription.active      # True
subscription.cancel()    # idempotent
```

Internally the registry is a list of entries in global registration order:

```python
@dataclass(frozen=True, slots=True)
class _Entry:
    event_type: type[Any]
    handler: Handler
    subscription: Subscription
```

A dict keyed by event type would make lookup cheaper for exact matches, but
routing is defined by the event class MRO, and handler order is global across
types. A list preserves both properties without a secondary ordering key.
Resolution is linear in the number of subscriptions, then cached per event
class.

## Routing, order, and the cache

A handler registered for `T` receives an event whose class is `T` or a subclass
of `T`. That is a straightforward `issubclass(event_type, entry.event_type)`
check over the ordered list. The result is a tuple of handlers:

```python
handlers = tuple(
    entry.handler
    for entry in self._entries
    if issubclass(event_type, entry.event_type)
)
```

The tuple is cached in a `WeakKeyDictionary` keyed by the event class. Weak
keys matter: applications often create event classes dynamically, and a
strong-keyed cache would keep every class alive forever. Any subscribe,
cancel, register, or unregister clears the cache. That makes mutation O(1) for
the cache but O(n) for rebuilding, which is the right trade for a bus where
subscriptions are rare and publications are frequent.

The global-order rule is worth making explicit. If `ChildEvent` and `BaseEvent`
both have handlers, and the base handler was registered first, the base handler
runs first. Herald does not group by specificity. That is deliberate: handler
ordering should be predictable, and "registration order" is easier to reason
about than an implicit specificity hierarchy.

## One lock, two phases

The concurrency model has one `RLock` and two phases per delivery.

Phase one, under the lock:

1. Reject the publish if the bus is closed.
2. Look up or build the cached handler tuple.
3. Return an immutable snapshot.

Phase two, after releasing the lock:

4. Invoke each handler in order.
5. Isolate exceptions.

The snapshot is the critical detail. Without it, a handler that publishes a
second event would try to re-acquire the same lock. `RLock` makes that
re-entrant on the same thread, but it would still hold the lock while arbitrary
user code runs, blocking every other thread. With a snapshot, handlers can call
`subscribe`, `cancel`, or `publish` freely.

The snapshot also defines the race semantics:

- A handler subscribed during dispatch does not receive the in-flight event,
  because it was not in the snapshot.
- A handler cancelled during dispatch may still receive the in-flight event,
  because it was already in the snapshot.

Both outcomes are deterministic enough to test, which is more important than
promising a particular cancellation behavior in a concurrent system.

Registration order under concurrency is defined by lock acquisition order.
There is no attempt to invent a global timestamp that would be meaningful
across threads.

## Failure is data, not control flow

Inside the delivery loop, each handler call is wrapped separately:

```python
for handler in handlers:
    try:
        handler(event)
    except Exception as exc:
        self._handle_failure(event, handler, exc)
```

Catching `Exception` rather than `BaseException` is intentional.
`KeyboardInterrupt` and `SystemExit` are shutdown signals, not delivery
failures. In `publish` they propagate to the caller. In `publish_async` the
executor records them on the returned future, and the remaining handlers in
that delivery are skipped.

An ordinary exception is logged at `ERROR` with its traceback, then converted:

```python
DeadEvent(event=event, handler=handler, exception=exc)
```

A `DeadEvent` is published on the same bus, synchronously, on the same thread
that observed the failure. That last part is not an implementation accident.
If async deliveries dead-lettered through `publish_async`, a saturated pool
could deadlock waiting for a worker that can never be created. Synchronous
dead-letter delivery keeps the failure path independent of pool capacity.

Dead letters are also generated for events with no matching subscribers. That
turns silent misconfiguration into an observable event:

```python
bus.subscribe(DeadEvent, report_unroutable_event)
```

The loop guard is simple: a `DeadEvent` is never dead-lettered. If a dead-letter
handler itself raises, Herald logs the failure and stops. If a `DeadEvent` has
no subscribers, it is dropped with a debug-level log entry. There is no
recursive failure amplification.

## Async without a second dispatcher

`publish_async` returns a `concurrent.futures.Future`, not an asyncio future:

```python
future = bus.publish_async(OrderPlaced("A-1002"))
future.result()
```

Each submission is one delivery. Handlers within that delivery run
sequentially, which preserves order and keeps error isolation simple. Separate
submissions can run concurrently up to `max_workers`.

Handler exceptions do not propagate through the future; they are dead-lettered.
That gives callers a clear contract: `future.result()` raises for bus-level
problems and `BaseException` signals, not for an individual handler's `ValueError`.
The latter is observable through the dead-letter channel.

For asyncio applications, `publish_awaitable` bridges the executor future
without pulling asyncio into the core import path:

```python
async def publish_order(bus: EventBus) -> None:
    await bus.publish_awaitable(OrderPlaced("A-1003"))
```

Coroutine handlers are a separate, optional adapter and are intentionally not
part of the core. Mixing an event loop into a thread-safe bus raises questions
about loop ownership, cancellation, and exception propagation that deserve
their own surface rather than a flag on `EventBus`.

## Shutdown is the hard part

Closing a concurrent bus has three problems: rejecting new work, not killing
in-flight work unexpectedly, and not deadlocking on your own worker.

`close(wait=True)` marks the bus closed under the lock, then calls
`executor.shutdown(wait=True)` outside the lock. Releasing the lock first is
essential: a worker finishing a delivery may need the lock for its handler
snapshot. Holding the lock while joining that worker is a deadlock.

`close(wait=False)` performs a non-blocking shutdown with
`cancel_futures=True`. Queued deliveries are cancelled and their futures report
`cancelled() == True`; running deliveries continue in the background.

The awkward case is `close(wait=True)` called from inside a handler that is
already running on the executor. Waiting would mean joining the current
thread. The executor exposes no public "am I in your pool?" check, so Herald
uses the thread-name prefix it controls:

```python
def _is_worker_thread(self) -> bool:
    return threading.current_thread().name.startswith(f"{self._name}_")
```

If the close originates on a bus worker, waiting is downgraded to a
non-blocking shutdown and a `WARNING` is logged. This is a heuristic, and it is
documented as one. A future version could track worker identities directly if
that becomes a problem, but the name prefix is stable because the bus creates
the executor with `thread_name_prefix=name`.

Close is idempotent. The first call performs the shutdown; later calls return
immediately regardless of `wait`.

## Discovery that fails early

The decorator API is small:

```python
@subscribe
def on_order(self, event: OrderPlaced) -> None: ...

@subscribe(OrderPlaced)
def on_anything(self, event: object) -> None: ...
```

Bare `@subscribe` records an "inferred" marker on the function. The explicit
form records the class. Registration is where the real validation happens:

1. Scan the class MRO from most-derived to base.
2. Within each class, use `vars(cls)` insertion order.
3. Skip names already seen, so a subclass override hides the base method even
   when the override is not decorated.
4. Bind the discovered function to the instance.
5. Require exactly one positional event parameter.
6. Resolve the parameter's type hint with `typing.get_type_hints`.

Step 6 handles `from __future__ import annotations`, where the annotation is a
string at runtime. It also means unresolved forward references fail at
`register` time with `SubscriberDefinitionError`, not at the first publish.
Failing registration is cheap; failing delivery is not.

The MRO plus `vars` rule gives deterministic order without relying on `dir()`,
which sorts names rather than preserving definition order. Subclass handlers
run before base handlers, and declaration order wins within a class.

## Type safety at the boundary

The public subscription method is generic:

```python
def subscribe(self, event_type: type[T], handler: Callable[[T], None]) -> Subscription: ...
```

That lets `mypy` verify that a handler accepts the event type it is registered
for. The implementation casts to an internal `Callable[[Any], None]` at the
boundary because the registry stores heterogeneous handlers. Runtime checks
cover what typing cannot: the event type must be a class, and the handler must
be callable.

Strict typing shaped a few internal choices. The cache is typed as
`WeakKeyDictionary[type[Any], tuple[Handler, ...]]`. Entries use a frozen
slotted dataclass. The async future is explicitly
`concurrent.futures.Future[None]`, so nobody mistakes it for an asyncio future.

## What the tests pin down

The suite covers dispatch order, MRO routing, duplicate subscriptions, cache
invalidation, dead-letter contents and loop guards, registration inheritance
and override behavior, invalid subscriber definitions, async completion and
concurrency, close races, re-entrant publish, subscription churn under load,
and weak-cache collection.

The tests that matter most are the ones that would catch a regression in the
concurrency contract:

- A handler publishing another event must not deadlock.
- A subscription added during dispatch must not see the in-flight event.
- A subscription cancelled during dispatch may still run from the snapshot.
- `close(wait=True)` from a worker must not deadlock.
- `close(wait=False)` must cancel queued deliveries without cancelling the
  running one.

Coverage is a means, not a goal, but the suite currently sits at 96% line
coverage. The remaining gaps are hard-to-trigger races rather than untested
public behavior.

## What Herald deliberately leaves out

Herald has no handler priorities, no retries, no persistence, no replay, and no
weak references to handlers. Those are not missing features so much as
boundary choices. Priorities complicate global ordering. Retries need
idempotency contracts. Persistence changes the failure model. Weak references
make subscription lifetime surprising.

The asyncio coroutine-handler adapter is the one open surface. The synchronous
and executor-backed paths are stable; the coroutine path needs an explicit
design for loop ownership and cancellation.

## Takeaways

A small event bus is mostly a story about boundaries:

- Take a snapshot before running user code.
- Catch `Exception`, not `BaseException`.
- Make failure observable as data, then stop the recursion.
- Keep the failure path off the pool that produced it.
- Release locks before joining threads.
- Validate discovery at registration time.
- Document the races you choose instead of pretending they do not exist.

Herald is available in this repository under the MIT license. The behavioral
contract lives in [`SPEC.md`](../SPEC.md), and the implementation lives in
[`src/herald/`](../src/herald/).
