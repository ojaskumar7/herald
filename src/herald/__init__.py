"""Herald: a lightweight in-process publish/subscribe event bus."""

from __future__ import annotations

from .bus import EventBus, Subscription
from .dead_letter import DeadEvent
from .errors import BusClosedError, HeraldError, SubscriberDefinitionError
from .subscriber import subscribe

__version__ = "0.1.0"

__all__ = [
    "BusClosedError",
    "DeadEvent",
    "EventBus",
    "HeraldError",
    "SubscriberDefinitionError",
    "Subscription",
    "__version__",
    "subscribe",
]
