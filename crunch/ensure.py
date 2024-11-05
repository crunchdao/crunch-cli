import logging
import types
import typing

import click
import pandas


def is_function(
    module: types.ModuleType,
    name: str,
    logger=logging.getLogger(),
):
    if not hasattr(module, name):
        logger.error("no `%s` function found", name)
        raise click.Abort()

    return getattr(module, name)


def is_dataframe(
    input: typing.Any,
    name: str,
    logger=logging.getLogger(),
):
    if not isinstance(input, pandas.DataFrame):
        logger.error(f"`%s` must be a dataframe", name)
        raise click.Abort()


def return_infer(
    result,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_names: list,
    logger=logging.getLogger()
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
        logger.error(f"prediction expected columns: `{expected}` but got `{got}`")
        raise click.Abort()

    return result
