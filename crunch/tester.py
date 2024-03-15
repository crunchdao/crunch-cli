import logging
import os
import time
import typing

import click
import pandas

from . import api, checker, command, constants, ensure, utils, monkey_patches

_logged_installed = False


def install_logger():
    global _logged_installed
    if _logged_installed:
        return

    import coloredlogs
    coloredlogs.install(
        level=logging.INFO,
        fmt='%(asctime)s %(message)s',
        datefmt='%H:%M:%S',
    )

    _logged_installed = True


def _monkey_patch_display():
    import builtins

    name = "display"
    if not hasattr(builtins, name):
        setattr(builtins, name, print)


def _run_timeseries(
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    has_gpu: bool,
    read_kwargs: dict,
    train_function: callable,
    infer_function: callable,
    embargo: int,
    number_of_features: int,
    split_keys: int,
    column_names: api.ColumnNames,
    x_train_path: str,
    y_train_path: str,
    x_test_path: str,
    y_test_path: str,
):
    full_x = pandas.concat([
        utils.read(x_train_path, kwargs=read_kwargs),
        utils.read(x_test_path, kwargs=read_kwargs),
    ])

    if y_test_path:
        full_y = pandas.concat([
            utils.read(y_train_path, kwargs=read_kwargs),
            utils.read(y_test_path, kwargs=read_kwargs),
        ])
    else:
        full_y = utils.read(y_train_path, kwargs=read_kwargs)

    for dataframe in [full_x, full_y]:
        dataframe.set_index(column_names.moon, drop=True, inplace=True)

    os.makedirs(model_directory_path, exist_ok=True)

    predictions: typing.List[pandas.DataFrame] = []

    moons = split_keys
    for index, moon in enumerate(moons):
        train = False
        if train_frequency != 0 and moon % train_frequency == 0:
            train = True
        elif index == 0 and force_first_train:
            train = True

        logging.warn('---')
        logging.warn(
            'loop: moon=%s train=%s (%s/%s)',
            moon, train, index + 1, len(moons)
        )

        default_values = {
            "number_of_features": number_of_features,
            "model_directory_path": model_directory_path,
            "id_column_name": column_names.id,
            "moon_column_name": column_names.moon,
            "target_column_name": column_names.target,
            "prediction_column_name": column_names.prediction,
            "column_names": column_names,
            "moon": moon,
            "current_moon": moon,
            "embargo": embargo,
            "has_gpu": has_gpu,
            "has_trained": train,
        }

        if train:
            logging.warn('call: train')
            x_train = full_x[full_x.index < moon - embargo].reset_index()
            y_train = full_y[full_y.index < moon - embargo].reset_index()

            utils.smart_call(train_function, default_values, {
                "X_train": x_train,
                "x_train": x_train,
                "Y_train": y_train,
                "y_train": y_train,
            })

        if True:
            logging.warn('call: infer')
            x_test = full_x[full_x.index == moon].reset_index()

            prediction = utils.smart_call(infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure.return_infer(
                prediction,
                column_names.id,
                column_names.moon,
                column_names.prediction,
            )

        predictions.append(prediction)

    return pandas.concat(predictions)


def _run_dag(
    model_directory_path: str,
    has_gpu: bool,
    read_kwargs: dict,
    train_function: callable,
    infer_function: callable,
    number_of_features: int,
    column_names: api.ColumnNames,
    x_train_path: str,
    y_train_path: str,
    x_test_path: str,
):
    x_train = utils.read(x_train_path, dataframe=False, kwargs=read_kwargs)
    x_test = utils.read(x_test_path, dataframe=False, kwargs=read_kwargs)
    y_train = utils.read(y_train_path, dataframe=False, kwargs=read_kwargs)

    os.makedirs(model_directory_path, exist_ok=True)

    default_values = {
        "number_of_features": number_of_features,
        "model_directory_path": model_directory_path,
        "id_column_name": column_names.id,
        "prediction_column_name": column_names.prediction,
        "column_names": column_names,
        "has_gpu": has_gpu,
        "has_trained": True,
    }

    if True:
        logging.warn('call: train')
        utils.smart_call(train_function, default_values, {
            "X_train": x_train,
            "x_train": x_train,
            "Y_train": y_train,
            "y_train": y_train,
        })

    if True:
        logging.warn('call: infer')
        prediction = utils.smart_call(infer_function, default_values, {
            "X_test": x_test,
            "x_test": x_test,
        })

        ensure.return_infer(
            prediction,
            column_names.id,
            column_names.moon,
            column_names.prediction,
        )

    return prediction


def run(
    module: typing.Any,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    competition_format: api.CompetitionFormat,
    has_gpu=False,
    checks=True,
    read_kwargs={},
    write_kwargs={},
):
    from .runner.local import LocalRunner
    runner = LocalRunner(
        module,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition_format,
        has_gpu,
        checks,
        read_kwargs,
        write_kwargs,
    )

    runner.start()
    # prediction = _run_dag(
    #     model_directory_path,
    #     has_gpu,
    #     read_kwargs,
    #     train_function,
    #     infer_function,
    #     number_of_features,
    #     column_names,
    #     x_train_path,
    #     y_train_path,
    #     x_test_path,
    # )