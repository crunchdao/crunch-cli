import typing

import pandas

from .. import vendor


def orthogonalize(
    competition_name: str,
    prediction: pandas.DataFrame,
    orthogonalization_data: typing.Any,
    id_column_name: str,
    moon_column_name: str,
    target_column_name: str,
    prediction_column_name: str,
):
    module = vendor.get(competition_name)

    orthogonalize = getattr(module, "orthogonalize", None)
    if orthogonalize is None:
        raise ValueError(f"no orthogonalize function for {competition_name}")

    dataframe = orthogonalize(
        competition_name=competition_name,
        prediction=prediction,
        orthogonalization_data=orthogonalization_data,
        id_column_name=id_column_name,
        moon_column_name=moon_column_name,
        target_column_name=target_column_name,
        prediction_column_name=prediction_column_name,
    )

    if not isinstance(dataframe, pandas.DataFrame):
        raise ValueError(f"orthogonalize returned: {dataframe.__class__.__name__}")

    return dataframe
