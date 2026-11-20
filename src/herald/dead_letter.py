"""Dead-letter event types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class DeadEvent:
    """Wraps an event that could not be delivered normally.

    ``handler`` and ``exception`` are populated when a handler raised. Both are
    ``None`` when no subscriber matched the original event.
    """

    event: object
    handler: Callable[..., None] | None = None
    exception: BaseException | None = None
