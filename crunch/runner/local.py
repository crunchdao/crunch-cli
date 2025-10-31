import logging
import os
import time
import typing

import click
import pandas

from .. import (api, checker, command, constants, container, unstructured, ensure,
                meta, monkey_patches, tester, utils)
from .collector import FilePredictionCollector, MemoryPredictionCollector, PredictionCollector
from .unstructured import RunnerContext, RunnerExecutorContext, UserModule
from .runner import Runner


class LocalRunner(Runner):

    def __init__(
        self,
        user_module: typing.Any,
        runner_module: unstructured.RunnerModule,
        model_directory_path: str,
        force_first_train: bool,
        train_frequency: int,
        round_number: str,
        competition: api.Competition,
        has_gpu: bool,
        checks: bool,
        determinism_check_enabled: bool,
        read_kwargs: dict,
        write_kwargs: dict,
        logger: logging.Logger,
    ):
        collector = (
            MemoryPredictionCollector()
            if not competition.format.unstructured
            else FilePredictionCollector()
        )

        super().__init__(
            collector,
            competition.format,
            determinism_check_enabled
        )

        self.user_module = user_module
        self.runner_module = runner_module
        self.model_directory_path = model_directory_path
        self.force_first_train = force_first_train
        self.train_frequency = train_frequency
        self.round_number = round_number
        self.has_gpu = has_gpu
        self.checks = checks
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs
        self.logger = logger

        self.metrics = competition.metrics.list()

    def start(self):
        memory_before = utils.get_process_memory()
        start = time.time()

        try:
            return super().start()
        finally:
            self.log(
                'duration - time=%s' % (
                    time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
                ),
                important=True,
            )

            memory_after = utils.get_process_memory()
            self.log(
                'memory - before="%s" after="%s" consumed="%s"' % (
                    utils.format_bytes(memory_before),
                    utils.format_bytes(memory_after),
                    utils.format_bytes(memory_after - memory_before)
                ),
                important=True,

            )

    def setup(self):
        tester.install_logger()
        monkey_patches.display_add()

    def initialize(self):
        self.log('running local test')
        self.log("internet access isn't restricted, no check will be done", important=True)
        self.log("")

        self.train_function = ensure.is_function(self.user_module, "train", logger=self.logger)
        self.infer_function = ensure.is_function(self.user_module, "infer", logger=self.logger)

        try:
            (
                self.embargo,
                self.number_of_features,
                self.keys,
                self.features,
                self.column_names,
                self.data_directory_path,
                self.data_paths,
            ) = command.download(
                round_number=self.round_number
            )

            if not self.competition_format.unstructured:
                self.x_train_path = self.data_paths.get(api.KnownData.X_TRAIN)
                self.y_train_path = self.data_paths.get(api.KnownData.Y_TRAIN)
                self.x_test_path = self.data_paths.get(api.KnownData.X_TEST)
                self.y_test_path = self.data_paths.get(api.KnownData.Y_TEST)
                self.example_prediction_path = self.data_paths.get(api.KnownData.EXAMPLE_PREDICTION)
        except (api.CrunchNotFoundException, api.MissingPhaseDataException):
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
        target_column_names, prediction_column_names = container.Columns.from_model(self.column_names)

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
            self.log('call: train', important=True)
            x_train = self.filter_embargo(self.full_x, moon)
            y_train = self.filter_embargo(self.full_y, moon)

            utils.smart_call(
                self.train_function,
                default_values,
                {
                    "X_train": x_train,
                    "x_train": x_train,
                    "Y_train": y_train,
                    "y_train": y_train,
                },
                logger=self.logger,
            )

        if True:
            self.log('call: infer', important=True)
            x_test = self.filter_at(self.full_x, moon)

            prediction = utils.smart_call(
                self.infer_function,
                default_values,
                {
                    "X_test": x_test,
                    "x_test": x_test,
                },
                logger=self.logger,
            )

            ensure.return_infer(
                prediction,
                self.column_names.id,
                self.column_names.moon,
                self.column_names.outputs,
                logger=self.logger,
            )

        return prediction

    def start_unstructured(self):
        if self.runner_module is None:
            self.log("no runner is available for this competition", error=True)
            raise click.Abort()

        context = LocalRunnerContext(self)

        prediction = self.runner_module.run(
            context=context,
            data_directory_path=self.data_directory_path,
            model_directory_path=self.model_directory_path,
            prediction_directory_path=self.prediction_directory_path,
        )

        return prediction

    def finalize(self):
        prediction_path = os.path.join(
            constants.DOT_DATA_DIRECTORY,
            "prediction.parquet"
        )

        self.log('save prediction - path=%s' % prediction_path, important=True)
        if self.prediction_collector is not None:
            self.prediction_collector.persist(prediction_path)

        if self.checks and not self.competition_format.unstructured:
            prediction = utils.read(prediction_path)
            example_prediction = utils.read(self.example_prediction_path)

            try:
                checker.run_via_api(
                    prediction,
                    example_prediction,
                    self.column_names,
                    self.logger,
                )

                self.log(f"prediction is valid", important=True)
            except checker.CheckError as error:
                if not isinstance(error.__cause__, checker.CheckError):
                    self.logger.exception(
                        "check failed - message=`%s`",
                        error,
                        exc_info=error.__cause__
                    )
                else:
                    self.log("check failed - message=`%s`" % error, error=True)

                return None

    def log(self, message: str, important=False, error=False):
        if error:
            self.logger.error(message)
        elif important:
            self.logger.warning(message)
        else:
            self.logger.info(message)

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


class LocalRunnerContext(RunnerContext):

    def __init__(self, runner: LocalRunner):
        self.runner = runner

    @property
    def force_first_train(self):
        return self.runner.force_first_train

    @property
    def is_local(self):
        return True

    @property
    def is_determinism_check_enabled(self):
        return self.runner.determinism_check_enabled

    def report_determinism(self, deterministic: bool):
        self.runner.deterministic = deterministic

    def log(
        self,
        message: str,
        important=False,
        error=False
    ):
        self.runner.log(message, important=important, error=error)

    def execute(
        self,
        command,
        parameters=None,
        return_prediction=False
    ):
        self.log(f"executing - command={command}")

        user_module = LocalUserModule(self.runner)
        executor_context = LocalRunnerExecutorContext(self.runner)

        handlers = self.runner.runner_module.execute(
            executor_context,
            user_module,
            self.runner.data_directory_path,
            self.runner.model_directory_path,
        )

        handler = handlers.get(command)
        if handler is None:
            self.log(f"command not found: {command}", error=True)
            return None

        result = utils.smart_call(
            handler,
            parameters or {},
        )

        if isinstance(return_prediction, PredictionCollector):
            ensure.is_dataframe(result, "result", logger=self.runner.logger)
            return_prediction.append(result)
        elif return_prediction == True:
            ensure.is_dataframe(result, "result", logger=self.runner.logger)
            return result


class LocalRunnerExecutorContext(RunnerExecutorContext):

    def __init__(self, runner: LocalRunner):
        self.runner = runner

    @property
    def is_local(self):
        return True

    def trip_data_fuse(self):
        pass  # no fuse locally


class LocalUserModule(UserModule):

    def __init__(self, runner: LocalRunner):
        self.runner = runner

    @property
    def module(self):
        return self.runner.user_module
