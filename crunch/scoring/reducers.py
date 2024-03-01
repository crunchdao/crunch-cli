import statistics
import typing

import numpy

from .. import api


def cumprod_plus_minus_1(
    values: typing.List[float]
) -> float:
    array = numpy.array(values)
    cumprod = numpy.cumprod(array + 1) - 1

    return cumprod[-1]


REGISTRY = {
    api.ReducerFunction.MEAN: statistics.mean,
    api.ReducerFunction.CUMPROD_PLUS_MINUS_1: cumprod_plus_minus_1,
}
