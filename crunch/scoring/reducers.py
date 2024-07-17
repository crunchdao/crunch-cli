import statistics
import typing

import numpy

from .. import api


def none(
    values: typing.List[float]
) -> float:
    raise NotImplementedError("scorer must be called instead")


def product_plus_minus_1(
    values: typing.List[float]
) -> float:
    array = numpy.array(values)
    return numpy.prod(array + 1) - 1


REGISTRY = {
    api.ReducerFunction.NONE: none,
    api.ReducerFunction.MEAN: statistics.mean,
    api.ReducerFunction.PRODUCT_PLUS_MINUS_1: product_plus_minus_1,
}
