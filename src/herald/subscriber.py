"""The ``@subscribe`` decorator and subscriber discovery."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast, get_type_hints, overload

from .errors import SubscriberDefinitionError

T = TypeVar("T")
P = ParamSpec("P")
Handler = Callable[..., None]

_ATTR = "__herald_event_type__"
_UNSET = object()
_INFERRED = object()


@overload
def subscribe(handler: Callable[P, None], /) -> Callable[P, None]: ...


@overload
def subscribe(event_type: type[T], /) -> Callable[[Callable[P, None]], Callable[P, None]]: ...


def subscribe(arg: object) -> object:
    """Mark a method as an event handler.

    Used bare (``@subscribe``) the event type is inferred from the handler's
    parameter hint at registration time. Used with an argument
    (``@subscribe(EventType)``) the event type is explicit.
    """
    if isinstance(arg, type):
        event_type = cast(type[Any], arg)

        def decorate(handler: Callable[..., None]) -> Callable[..., None]:
            return _mark(handler, event_type)

        return decorate
    if callable(arg):
        return _mark(cast(Handler, arg), _INFERRED)
    raise TypeError("@subscribe expects an event type or a callable")


def _mark(handler: Handler, event_type: object) -> Handler:
    setattr(handler, _ATTR, event_type)
    return handler


def collect_subscribers(instance: object) -> list[tuple[type[Any], Handler]]:
    """Return ``(event_type, bound_handler)`` pairs for ``instance``.

    Scans the class MRO from most-derived to base, and each class in definition
    order. An overridden method hides the base implementation, including when
    the override is not decorated.
    """
    subscribers: list[tuple[type[Any], Handler]] = []
    seen: set[str] = set()
    for cls in type(instance).__mro__:
        for name, attribute in vars(cls).items():
            if name in seen:
                continue
            seen.add(name)
            if isinstance(attribute, (staticmethod, classmethod)):
                raw = attribute.__func__
            else:
                raw = attribute
            marker = getattr(raw, _ATTR, _UNSET)
            if marker is _UNSET:
                continue
            bound = getattr(instance, name)
            explicit = None if marker is _INFERRED else cast(type[Any], marker)
            event_type = _resolve_event_type(bound, explicit, name)
            subscribers.append((event_type, cast(Handler, bound)))
    return subscribers


def _resolve_event_type(handler: object, explicit: type[Any] | None, name: str) -> type[Any]:
    if not callable(handler):
        raise SubscriberDefinitionError(f"@subscribe method {name!r} is not callable")
    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError) as exc:
        raise SubscriberDefinitionError(f"cannot inspect @subscribe method {name!r}") from exc
    parameters = list(signature.parameters.values())
    valid_kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    if len(parameters) != 1 or parameters[0].kind not in valid_kinds:
        raise SubscriberDefinitionError(
            f"@subscribe method {name!r} must accept exactly one event argument"
        )
    if explicit is not None:
        return explicit
    parameter = parameters[0]
    try:
        hints = get_type_hints(handler)
    except Exception as exc:
        raise SubscriberDefinitionError(
            f"cannot resolve type hints for @subscribe method {name!r}"
        ) from exc
    annotation = hints.get(parameter.name)
    if annotation is None:
        raise SubscriberDefinitionError(
            f"@subscribe method {name!r} has no event type hint; "
            "pass one explicitly with @subscribe(EventType)"
        )
    if not isinstance(annotation, type):
        raise SubscriberDefinitionError(
            f"@subscribe method {name!r} has a non-class event type hint {annotation!r}"
        )
    return annotation
