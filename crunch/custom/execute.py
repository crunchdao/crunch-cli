
import typing

from .. import utils


class ParticipantVisibleError(Exception):
    pass


def call_function(
    function: typing.Callable,
    kwargs: dict,
    print: typing.Optional[typing.Callable[[typing.Any], None]] = None,
):
    try:
        if print:
            print(f"\n\ncalling {function}\n")

        return utils.smart_call(
            function,
            kwargs,
        )
    except Exception as exception:
        if exception.__class__.__name__ == 'ParticipantVisibleError':
            raise ParticipantVisibleError(str(exception)) from exception

        raise
