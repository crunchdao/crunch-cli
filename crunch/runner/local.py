import logging
import os
import time
import typing

import click
import pandas

from .. import (api, checker, command, constants, ensure, monkey_patches,
                tester, utils)
from ..container import CallableIterable, Columns, GeneratorWrapper
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
        determinism_check_enabled=False,
        read_kwargs={},
        write_kwargs={},
    ):
        super().__init__(competition_format, determinism_check_enabled)

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
        memory_before = utils.get_process_memory()
        start = time.time()

        try:
            return super().start()
        finally:
            logging.warning(
                'duration - time=%s',
                time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
            )

            memory_after = utils.get_process_memory()
            logging.warning(
                'memory - before="%s" after="%s" consumed="%s"',
                utils.format_bytes(memory_before),
                utils.format_bytes(memory_after),
                utils.format_bytes(memory_after - memory_before)
            )

    def setup(self):
        tester.install_logger()
        monkey_patches.display_add()

    def initialize(self):
        logging.info('running local test')
        logging.warning("internet access isn't restricted, no check will be done")
        logging.info("")

        self.train_function = ensure.is_function(self.module, "train")
        self.infer_function = ensure.is_function(self.module, "infer")

        try:
            (
                self.embargo,
                self.number_of_features,
                self.keys,
                self.features,
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

    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        target_column_names, prediction_column_names = Columns.from_model(self.column_names)

        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "id_column_name": self.column_names.id,
            "moon_column_name": self.column_names.moon,
            "target_column_name": self.column_names.first_target.input,
            "target_column_names": target_column_names,
            "prediction_column_name": self.column_names.first_target.output,
            "prediction_column_names": prediction_column_names,
            "column_names": self.column_names,
            "moon": moon,
            "current_moon": moon,
            "embargo": self.embargo,
            "has_gpu": self.has_gpu,
            "has_trained": train,
            **self.features.to_parameter_variants(),
        }

        if train:
            logging.warning('call: train')
            x_train = self.filter_embargo(self.full_x, moon)
            y_train = self.filter_embargo(self.full_y, moon)

            utils.smart_call(self.train_function, default_values, {
                "X_train": x_train,
                "x_train": x_train,
                "Y_train": y_train,
                "y_train": y_train,
            })

        if True:
            logging.warning('call: infer')
            x_test = self.filter_at(self.full_x, moon)

            prediction = utils.smart_call(self.infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure.return_infer(
                prediction,
                self.column_names.id,
                self.column_names.moon,
                self.column_names.outputs,
            )

        return prediction

    def dag_loop(
        self,
        train: bool
    ):
        x_train = utils.read(self.x_train_path, kwargs=self.read_kwargs)
        x_test = utils.read(self.x_test_path, kwargs=self.read_kwargs)
        y_train = utils.read(self.y_train_path, kwargs=self.read_kwargs)

        _, prediction_column_names = Columns.from_model(self.column_names)

        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "id_column_name": self.column_names.id,
            "prediction_column_name": self.column_names.first_target.output,
            "prediction_column_names": prediction_column_names,
            "column_names": self.column_names,
            "has_gpu": self.has_gpu,
            "has_trained": train,
        }

        if train:
            logging.warning('call: train')
            utils.smart_call(self.train_function, default_values, {
                "X_train": x_train,
                "x_train": x_train,
                "Y_train": y_train,
                "y_train": y_train,
            })

        if True:
            logging.warning('call: infer')
            prediction = utils.smart_call(self.infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure.return_infer(
                prediction,
                self.column_names.id,
                None,
                self.column_names.outputs,
            )

        return prediction

    def start_stream(self):
        self.x_test = utils.read(self.x_test_path, kwargs=self.read_kwargs)
        # self.x_test = pandas.concat([
        #     utils.read(self.x_train_path, kwargs=self.read_kwargs),
        #     self.x_test
        # ])

        return super().start_stream()

    def _get_stream_default_values(self):
        return {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "column_names": self.column_names,
            "embargo": self.embargo,
            "has_gpu": self.has_gpu,
            "horizon": 2,  # TODO load from competition parameters
            **self.features.to_parameter_variants(),
        }

    def stream_have_model(self):
        return True

    def stream_no_model(
        self,
    ):
        default_values = self._get_stream_default_values()
        stream_names = set(self.column_names.target_names)

        x_train = utils.read(self.x_train_path, kwargs=self.read_kwargs)

        streams = []
        for stream_name, group in x_train.groupby(self.column_names.id):
            if stream_name not in stream_names:
                continue

            parts = utils.split_at_nans(group, self.column_names.side)
            for part in parts:
                streams.append(CallableIterable.from_dataframe(part, self.column_names.side))

        logging.warning(f'call: train - stream.len={len(streams)}')

        utils.smart_call(
            self.train_function,
            default_values,
            {
                "streams": streams,
            }
        )

    def stream_loop(
        self,
        target_column_name: api.TargetColumnNames,
    ) -> pandas.DataFrame:
        default_values = self._get_stream_default_values()

        x_data = self.x_test[[
            self.column_names.moon,
            self.column_names.id,
            self.column_names.side,
        ]]

        x_data = x_data[x_data[self.column_names.id] == target_column_name.name]

        stream_datas = [
            part[self.column_names.side]
            for part in utils.split_at_nans(x_data, self.column_names.side)
        ]

        predicteds = []
        for index, stream_data in enumerate(stream_datas):
            logging.warning(f'call: infer ({index+ 1}/{len(stream_datas)})')

            wrapper = GeneratorWrapper(
                iter(stream_data),
                lambda stream: utils.smart_call(
                    self.infer_function,
                    default_values,
                    {
                        "stream": stream,
                    }
                )
            )

            collecteds = wrapper.collect(len(stream_data))
            predicteds.extend(collecteds)

        x_data.dropna(subset=[self.column_names.side], inplace=True)

        return pandas.DataFrame({
            self.column_names.moon: x_data[self.column_names.moon],
            self.column_names.id: x_data[self.column_names.id],
            target_column_name.output: predicteds,
        })

    def finalize(self, prediction: pandas.DataFrame):
        prediction_path = os.path.join(
            constants.DOT_DATA_DIRECTORY,
            "prediction.csv"
        )

        logging.warning('save prediction - path=%s', prediction_path)
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

                logging.warning(f"prediction is valid")
            except checker.CheckError as error:
                if not isinstance(error.__cause__, checker.CheckError):
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
