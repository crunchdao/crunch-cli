import logging
import types
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


def is_number(input, name: str):
    if not isinstance(input, (int, float)):
        logging.error(f"`%s` must be a number", name)
        raise click.Abort()


def is_generator(input, name: str):
    if not isinstance(input, types.GeneratorType):
        logging.error(f"`%s` must use yield", name)
        raise click.Abort()


def return_infer(
    result,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_names: list
) -> pandas.DataFrame:
    is_dataframe(result, "prediction")

    # TODO should not happen, but should we still use a set here?
    expected = {
        id_column_name,
        *prediction_column_names
    }

    if moon_column_name is not None:
        expected.add(moon_column_name)
    
    got: typing.Set[str] = set(result.columns)

    if got != expected:
        logging.error(f"prediction expected columns: `{expected}` but got `{got}`")
        raise click.Abort()

    return result
