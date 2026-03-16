from typing import Any, Callable, Dict, Optional, TypeVar

from crunch.utils import LazyValue, smart_call

_T = TypeVar("_T")


class ParticipantVisibleError(Exception):
    pass


def call_function(
    function: Callable[..., _T],
    kwargs: Dict[str, Any] = {},
    *,
    lazy_kwargs: Dict[str, Callable[[], Any]] = {},
    print: Optional[Callable[..., None]] = None,
) -> _T:
    try:
        if print:
            print(f"\n\ncalling {function}\n")

        return smart_call(
            function,
            kwargs,
            {
                name: LazyValue(factory)
                for name, factory in lazy_kwargs.items()
            },
        )
    except Exception as exception:
        if exception.__class__.__name__ == 'ParticipantVisibleError':
            raise ParticipantVisibleError(str(exception)) from exception

        raise
