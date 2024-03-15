import numpy
import pandas

from .. import api


class CheckError(ValueError):
    pass


def columns_name(
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame
):
    left = prediction.columns.tolist()
    right = example_prediction.columns.tolist()

    if set(left) != set(right):
        raise CheckError("Columns name are different from what is expected")


def nans(
    prediction: pandas.DataFrame
):
    if prediction.isna().sum().sum() > 0:
        raise CheckError("NaNs detected")

    prediction = prediction.replace([numpy.inf, -numpy.inf], numpy.nan)
    if prediction.isna().sum().sum() > 0:
        raise CheckError("inf detected")


def values_between(
    prediction: pandas.DataFrame,
    prediction_column_name: str,
    min: float,
    max: float,
):
    has_less = (prediction.loc[:, prediction_column_name].values < min).any()
    has_more = (prediction.loc[:, prediction_column_name].values > max).any()

    if has_less or has_more:
        raise CheckError(f"Values are not between {min} and {max}")


def values_allowed(
    prediction: pandas.DataFrame,
    prediction_column_name: str,
    values: list
):
    if not prediction[prediction_column_name].isin(values).all():
        raise CheckError(f"Values should only be: {values}")


def moons(
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    moon_column_name: str,
):
    left = prediction[moon_column_name].unique()
    right = example_prediction[moon_column_name].unique()

    if set(left) != set(right):
        raise CheckError(f"{moon_column_name} are different from what is expected")


# TODO Maybe it would be better to extract ids first, and then do those checks?
def ids(
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    competition_format: api.CompetitionFormat,
):
    """
    DataFrame must already be filtered.
    """

    def extract_ids(dataframe: pandas.DataFrame):
        column = dataframe[id_column_name]

        if competition_format == api.CompetitionFormat.TIMESERIES:
            return column

        # TODO Too much specific
        if competition_format == api.CompetitionFormat.DAG:
            id_format = f"^(\\d+)_.*?$"
            id_infos = example_prediction[id_column_name].str.extract(id_format)

            return id_infos[0]

        raise ValueError(f"unsupported competition format: {competition_format}")

    left = extract_ids(prediction)
    if competition_format != api.CompetitionFormat.DAG:
        if left.duplicated().sum() > 0:
            raise CheckError(f"Duplicate ID(s)")

    right = extract_ids(example_prediction)
    if set(left) != set(right):
        raise CheckError(f"Different ID(s)")


def constants(
    prediction: pandas.DataFrame,
    prediction_column_name: str,
):
    """
    DataFrame must already be filtered.
    """

    n = prediction[prediction_column_name].nunique()

    if n == 1:
        raise CheckError(f"Constant values")


REGISTRY = {
    api.CheckFunction.COLUMNS_NAME: columns_name,
    api.CheckFunction.NANS: nans,
    api.CheckFunction.VALUES_BETWEEN: values_between,
    api.CheckFunction.VALUES_ALLOWED: values_allowed,
    api.CheckFunction.MOONS: moons,
    api.CheckFunction.IDS: ids,
    api.CheckFunction.CONSTANTS: constants,
}
