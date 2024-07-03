import typing

import pandas

from ... import api


def merge(
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    metrics: typing.List[api.Metric],
):
    target_ids = {
        metric.target.id
        for metric in metrics
    }

    prediction = prediction[[
        column_names.moon,
        column_names.id,
        *{
            target.output
            for target in column_names.targets
            if target.id in target_ids
        }
    ]]

    y_test = y_test[[
        column_names.moon,
        column_names.id,
        *{
            target.input
            for target in column_names.targets
            if target.id in target_ids
        }
    ]]

    return pandas.merge(
        y_test,
        prediction,
        on=[
            column_names.moon,
            column_names.id
        ]
    )
