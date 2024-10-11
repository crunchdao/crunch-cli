import typing

import pandas

from ... import api, meta


def merge(
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    metrics: typing.List[api.Metric],
):
    time_meta_metrics = meta.filter_metrics(metrics, None, api.ScorerFunction.META__EXECUTION_TIME)

    prediction = prediction[[
        column_names.moon,
        column_names.id,
        column_names.output,
        *list({
            meta.to_column_name(metric, column_names.output)
            for metric in time_meta_metrics
        })
    ]]

    y_test = y_test[[
        column_names.moon,
        column_names.id,
        column_names.input,
    ]]

    y_test.dropna(inplace=True)

    groups = []
    for stream_name, x_test_group in y_test.groupby(column_names.id):
        x_test_group.reset_index(drop=True, inplace=True)

        group = prediction[prediction[column_names.id] == stream_name].reset_index(drop=True)
        group[column_names.input] = x_test_group[column_names.input]

        assert not group[column_names.output].isna().sum()  # TODO Should be assured by checks instead
        groups.append(group)

    return pandas.concat(groups)
