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
    def process(group: pandas.DataFrame):
        date = group.name

        for prediction_column_name in column_names.outputs:
            group[prediction_column_name] = _gaussianizer(group[prediction_column_name])
            group[prediction_column_name] = _orthogonalizer(group, orthogonalization_data[date], column_names.id, prediction_column_name)
            # Based on orthogonalization_data, necessary to bring back to mean zero before L1-normalizing.
            group[prediction_column_name] = _mean_zeroed(group[prediction_column_name])
            group[prediction_column_name] = _l1_normalize(group[prediction_column_name])

        return group

    return prediction\
        .groupby(column_names.moon, group_keys=False)\
        .apply(process)


def _gaussianizer(
    series: pandas.Series,
) -> pandas.Series:
    normalized_rank = series.rank(method="average").values / (len(series) + 1)
    gaussianized_values = scipy.stats.norm.ppf(normalized_rank)

    return pandas.Series(
        gaussianized_values, 
        index=series.index
    )

def _orthogonalizer(
    prediction: pandas.DataFrame,
    P_matrix: pandas.DataFrame,
    id_column_name: str,
    prediction_column_name: str,
) -> pandas.Series:
    projector_columns = [
        column
        for column in P_matrix.columns
        if 'Projector' in column
    ]

    merged = pandas.merge(
        prediction,
        P_matrix,
        on=id_column_name,
        how='right' # necessary to preserve the order of the rows of the projection matrix.
    )

    prediction_parallel = numpy.dot(
        merged[projector_columns],
        merged[prediction_column_name]
    )

    return merged[prediction_column_name].to_numpy() - prediction_parallel


def _mean_zeroed(
    series: pandas.Series
) -> pandas.Series:
    return series - series.mean()


def _l1_normalize(
    series: pandas.Series
) -> pandas.Series:
    return series / numpy.linalg.norm(series, ord=1)
