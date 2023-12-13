import logging
import os
import time
import typing

import click
import coloredlogs
import pandas
import requests

from . import api, checker, command, constants, ensure, utils

_logged_installed = False


def install_logger():
    global _logged_installed
    if _logged_installed:
        return

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


def run(
    module: typing.Any,
    session: requests.Session,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    has_gpu=False,
    checks=True,
    read_kwargs={},
    write_kwargs={},
):
    install_logger()
    _monkey_patch_display()

    logging.info('running local test')
    logging.warn("internet access isn't restricted, no check will be done")
    logging.info("")

    memory_before = utils.get_process_memory()
    start = time.time()

    train_function = ensure.is_function(module, "train")
    infer_function = ensure.is_function(module, "infer")

    try:
        (
            embargo,
            number_of_features,
            (
                id_column_name,
                moon_column_name,
                target_column_name,
                prediction_column_name,
            ),
            (
                x_train_path,
                y_train_path,
                x_test_path,
                y_test_path,
                example_prediction_path
            )
        ) = command.download(
            session,
            round_number=round_number
        )
    except api.CurrentCrunchNotFoundException:
        command.download_no_data_available()
        raise click.Abort()

    try:
        x_test = utils.read(x_test_path)
        moons = x_test[moon_column_name].unique()
        moons.sort()

        full_x = pandas.concat([
            utils.read(x_train_path, kwargs=read_kwargs),
            x_test,
        ])

        if y_test_path:
            full_y = pandas.concat([
                utils.read(y_train_path, kwargs=read_kwargs),
                utils.read(y_test_path, kwargs=read_kwargs),
            ])
        else:
            full_y = utils.read(y_train_path, kwargs=read_kwargs)

        for dataframe in [full_x, full_y]:
            dataframe.set_index(moon_column_name, drop=True, inplace=True)

        del x_test

        os.makedirs(model_directory_path, exist_ok=True)

        predictions: typing.List[pandas.DataFrame] = []

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
                "id_column_name": id_column_name,
                "moon_column_name": moon_column_name,
                "target_column_name": target_column_name,
                "prediction_column_name": prediction_column_name,
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
                    id_column_name,
                    moon_column_name,
                    prediction_column_name,
                )

            predictions.append(prediction)

        prediction = pandas.concat(predictions)
        prediction_path = os.path.join(
            constants.DOT_DATA_DIRECTORY,
            "prediction.csv"
        )

        logging.warn('save prediction - path=%s', prediction_path)
        utils.write(prediction, prediction_path, kwargs=write_kwargs)

        if checks:
            example_prediction = utils.read(example_prediction_path)

            try:
                checker.run_via_api(
                    session,
                    prediction,
                    example_prediction,
                    id_column_name,
                    moon_column_name,
                    prediction_column_name,
                )

                logging.warn(f"prediction is valid")
            except checker.CheckError as error:
                if error.__cause__:
                    logging.exception(
                        "check failed - message=`%s`",
                        error,
                        exc_info=error.__cause__
                    )
                else:
                    logging.error("check failed - message=`%s`", error)

                return None

        return prediction
    finally:
        logging.warn(
            'duration - time=%s',
            time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
        )

        memory_after = utils.get_process_memory()
        logging.warn(
            'memory - before="%s" after="%s" consumed="%s"',
            utils.format_bytes(memory_before),
            utils.format_bytes(memory_after),
            utils.format_bytes(memory_after - memory_before)
        )
