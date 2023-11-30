import logging
import typing

import click
import pandas


def is_function(module, name: str):
    if not hasattr(module, name):
        logging.error("no `%s` function found", name)
        raise click.Abort()

    return getattr(module, name)


def is_dataframe(input, name: str):
    if not isinstance(input, pandas.DataFrame):
        logging.error(f"`%s` must be a dataframe", name)
        raise click.Abort()


def return_infer(
    result,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str
) -> pandas.DataFrame:
    is_dataframe(result, "prediction")

    expected = {
        id_column_name,
        moon_column_name,
        prediction_column_name
    }
    
    got: typing.Set[str] = set(result.columns)

    if got != expected:
        logging.error(f"prediction expected columns: `{expected}` but got `{got}`")
        raise click.Abort()

    return result
