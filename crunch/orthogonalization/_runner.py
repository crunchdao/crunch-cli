import typing

import pandas

from .. import api


def _not_available(dataframe: pandas.DataFrame) -> typing.List[api.Score]:
    raise NotImplementedError("orthogonalization is not available")


# runner's executor will replace it
delegate = _not_available


def set(f: typing.Callable[[pandas.DataFrame], typing.List[api.Score]]):
    global delegate
    delegate = f


def restore():
    global delegate
    delegate = _not_available
