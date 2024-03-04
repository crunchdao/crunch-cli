import typing

import numpy
import pandas
import scipy.stats

from .. import api


def orthogonalize(
    prediction: pandas.DataFrame,
    orthogonalization_data: typing.Any,
    column_names: api.ColumnNames,
    **kwargs
):
    prediction_column_name = column_names.prediction

    def process(group: pandas.DataFrame):
        date = group.name

        group[prediction_column_name] = _gaussianizer(group[prediction_column_name])
        group[prediction_column_name] = _orthogonalizer(group, orthogonalization_data[date], column_names.id, prediction_column_name)
        group[prediction_column_name] = _mean_zeroed(group[prediction_column_name])
        group[prediction_column_name] = _linealg_norm(group[prediction_column_name])

        return group

    return prediction\
        .groupby(column_names.moon, group_keys=False)\
        .apply(process)


def _gaussianizer(
    series: pandas.Series,
    range=3.,
) -> pandas.Series:
    """
    :param range: (float) The range for generating alpha scores.
    """
    series_length = len(series)

    x = numpy.linspace(-range, range, series_length)
    cdf_vector = scipy.stats.norm.cdf(x)

    xx = numpy.linspace(cdf_vector[0], cdf_vector[-1], series_length)
    gaussian = scipy.stats.norm.ppf(xx)

    gaussian -= numpy.mean(gaussian)
    gaussian /= numpy.std(gaussian)

    indices = series.rank(method="first").astype(int)

    return pandas.Series(
        data=gaussian[indices - 1],
        index=series.index
    )


def _orthogonalizer(
    prediction: pandas.DataFrame,
    betas: pandas.DataFrame,
    id_column_name: str,
    prediction_column_name: str,
) -> pandas.Series:
    projector_columns = [
        column
        for column in betas.columns
        if 'Projector' in column
    ]

    merged = pandas.merge(
        prediction,
        betas,
        on=id_column_name
    )

    dot = numpy.dot(
        merged[projector_columns],
        merged[prediction_column_name]
    )

    return prediction[prediction_column_name] - dot


def _mean_zeroed(
    series: pandas.Series
) -> pandas.Series:
    return series - series.mean()


def _linealg_norm(
    series: pandas.Series
) -> pandas.Series:
    return series / numpy.linalg.norm(series, ord=1)
