import pandas

from ... import api


def merge(
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
):
    prediction = prediction[[
        column_names.moon,
        column_names.id,
        column_names.output,
    ]]

    y_test = y_test[[
        column_names.moon,
        column_names.id,
        column_names.input,
    ]]

    return pandas.merge(
        y_test,
        prediction,
        on=[
            column_names.moon,
            column_names.id
        ]
    )
