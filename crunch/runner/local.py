import logging
import os
import time
import typing

import click
import pandas

from .. import (api, checker, command, constants, ensure, monkey_patches,
                tester, utils)
from .runner import Runner


class LocalRunner(Runner):

    def __init__(
        self,
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
        super().__init__(competition_format)

        self.module = module
        self.model_directory_path = model_directory_path
        self.force_first_train = force_first_train
        self.train_frequency = train_frequency
        self.round_number = round_number
        self.has_gpu = has_gpu
        self.checks = checks
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs

    def start(self):
        self.report_current("starting")

        super().start()

        self.report_current("ending")

    def setup(self):
        tester.install_logger()
        monkey_patches.display_add()

    def initialize(self):
        logging.info('running local test')
        logging.warn("internet access isn't restricted, no check will be done")
        logging.info("")

        self.train_function = ensure.is_function(self.module, "train")
        self.infer_function = ensure.is_function(self.module, "infer")

        try:
            (
                self.embargo,
                self.number_of_features,
                self.keys,
                self.column_names,
                (
                    self.x_train_path,
                    self.y_train_path,
                    self.x_test_path,
                    self.y_test_path,
                    self.example_prediction_path
                )
            ) = command.download(
                round_number=self.round_number
            )
        except api.CrunchNotFoundException:
            command.download_no_data_available()
            raise click.Abort()

        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            self.full_x = pandas.concat([
                utils.read(self.x_train_path, kwargs=self.read_kwargs),
                utils.read(self.x_test_path, kwargs=self.read_kwargs),
            ])

            if self.y_test_path:
                self.full_y = pandas.concat([
                    utils.read(self.y_train_path, kwargs=self.read_kwargs),
                    utils.read(self.y_test_path, kwargs=self.read_kwargs),
                ])
            else:
                self.full_y = utils.read(self.y_train_path, kwargs=self.read_kwargs)

            for dataframe in [self.full_x, self.full_y]:
                dataframe.set_index(self.column_names.moon, drop=True, inplace=True)

        os.makedirs(self.model_directory_path, exist_ok=True)

        return (
            self.keys,
            False,
        )

    def start(self):
        memory_before = utils.get_process_memory()
        start = time.time()

        try:
            super().start()
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

    def start_dag(self):
        x_train = utils.read(self.x_train_path, dataframe=False, kwargs=self.read_kwargs)
        x_test = utils.read(self.x_test_path, dataframe=False, kwargs=self.read_kwargs)
        y_train = utils.read(self.y_train_path, dataframe=False, kwargs=self.read_kwargs)

        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "id_column_name": self.column_names.id,
            "prediction_column_name": self.column_names.prediction,
            "column_names": self.column_names,
            "has_gpu": self.has_gpu,
            "has_trained": True,
        }

        if True:
            logging.warn('call: train')
            utils.smart_call(self.train_function, default_values, {
                "X_train": x_train,
                "x_train": x_train,
                "Y_train": y_train,
                "y_train": y_train,
            })

        if True:
            logging.warn('call: infer')
            prediction = utils.smart_call(self.infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure.return_infer(
                prediction,
                self.column_names.id,
                None,
                self.column_names.prediction,
            )

        return prediction

    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "id_column_name": self.column_names.id,
            "moon_column_name": self.column_names.moon,
            "target_column_name": self.column_names.target,
            "prediction_column_name": self.column_names.prediction,
            "column_names": self.column_names,
            "moon": moon,
            "current_moon": moon,
            "embargo": self.embargo,
            "has_gpu": self.has_gpu,
            "has_trained": train,
        }

        if train:
            logging.warn('call: train')
            x_train = self.filter_embargo(self.full_x, moon)
            y_train = self.filter_embargo(self.full_y, moon)

            utils.smart_call(self.train_function, default_values, {
                "X_train": x_train,
                "x_train": x_train,
                "Y_train": y_train,
                "y_train": y_train,
            })

        if True:
            logging.warn('call: infer')
            x_test = self.filter_at(self.full_x, moon)

            prediction = utils.smart_call(self.infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure.return_infer(
                prediction,
                self.column_names.id,
                self.column_names.moon,
                self.column_names.prediction,
            )

        return prediction

    def finalize(self, prediction: pandas.DataFrame):
        prediction_path = os.path.join(
            constants.DOT_DATA_DIRECTORY,
            "prediction.csv"
        )

        logging.warn('save prediction - path=%s', prediction_path)
        utils.write(prediction, prediction_path, kwargs={
            "index": False,
            **self.write_kwargs
        })

        if self.checks:
            example_prediction = utils.read(self.example_prediction_path)

            try:
                checker.run_via_api(
                    prediction,
                    example_prediction,
                    self.column_names,
                    logging,
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

    def log(self, message: str, error=False):
        if error:
            logging.error(message)
        else:
            logging.info(message)

        return True

    def filter_embargo(
        self,
        dataframe: pandas.DataFrame,
        moon: int
    ):
        # TODO Use split's key with (index(moon) - embargo)
        return dataframe[dataframe.index < moon - self.embargo].reset_index()

    def filter_at(
        self,
        dataframe: pandas.DataFrame,
        moon: int
    ):
        return dataframe[dataframe.index == moon].reset_index()
