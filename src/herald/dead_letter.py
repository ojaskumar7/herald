"""Dead-letter event types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class DeadEvent:
    """Wraps an event that could not be delivered normally.

    ``handler`` and ``exception`` are populated when a handler raised. Both are
    ``None`` when no subscriber matched the original event.
    """

    event: object
    handler: Callable[..., None] | None = None
    exception: BaseException | None = None
