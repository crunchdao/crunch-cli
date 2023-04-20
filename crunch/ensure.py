import logging
import pandas
import click
import typing


def is_function(module, name: str):
    if not hasattr(module, name):
        logging.error("no `%s` function found", name)
        raise click.Abort()

    return getattr(module, name)


def is_tuple_3(input):
    if not isinstance(input, tuple):
        logging.error("result is not a tuple")
        raise click.Abort()

    if len(input) != 3:
        logging.error("result tuple must be of length 3")
        raise click.Abort()


def is_dataframe(input, name: str):
    if not isinstance(input, pandas.DataFrame):
        logging.error(f"`%s` must be a dataframe", name)
        raise click.Abort()


def return_data_process(result) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
    is_tuple_3(result)

    x_train, y_train, x_test = result
    is_dataframe(x_train, "x_train")
    is_dataframe(y_train, "y_train")
    is_dataframe(x_test, "x_test")

    return result


def return_infer(result) -> pandas.DataFrame:
    is_dataframe(result, "prediction")

    return result
