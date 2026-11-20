# Herald

A lightweight, in-process publish/subscribe event bus for Python.

Herald routes typed events to subscribed handlers, isolates handler failures,
and reports failed or unhandled deliveries as dead letters. It has no runtime
dependencies beyond the standard library.

```python
from herald import EventBus

bus = EventBus()
bus.subscribe(SomeEvent, handle_some_event)
bus.publish(SomeEvent())
```

See `SPEC.md` for the full behavioral specification and `docs/` for design
notes and the technical write-up.
