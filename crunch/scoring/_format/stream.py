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

    y_test.dropna(inplace=True)

    groups = []
    for stream_name, x_test_group in y_test.groupby(column_names.id):
        prediction_group = prediction[prediction[column_names.id] == stream_name]
        
        x_test_group.reset_index(drop=True, inplace=True)
        prediction_group.reset_index(drop=True, inplace=True)

        x_test_group[column_names.output] = prediction_group[column_names.output]

        assert not len(x_test_group[x_test_group[column_names.output].isna()])  # TODO Should be assured by checks instead
        groups.append(x_test_group)

    return pandas.concat(groups)
