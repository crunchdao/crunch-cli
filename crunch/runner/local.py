import logging
import os
import time
from typing import Any, Literal, Optional

import click
import pandas

import crunch.checker as checker
import crunch.ensure as ensure
import crunch.monkey_patches as monkey_patches
import crunch.tester as tester
from crunch.api import Competition, CrunchNotFoundException, KnownData, MissingPhaseDataException, RoundIdentifierType
from crunch.command import download, download_no_data_available
from crunch.container import Columns
from crunch.runner.runner import Runner
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerContext, RunnerExecutorContext, UserModule
from crunch.unstructured import RunnerModule
from crunch.utils import format_bytes, get_process_memory, read, smart_call


class LocalRunner(Runner):

    def __init__(
        self,
        user_module: Any,
        runner_module: Optional[RunnerModule],
        model_directory_path: str,
        prediction_directory_path: str,
        force_first_train: bool,
        train_frequency: int,
        round_number: RoundIdentifierType,
        competition: Competition,
        has_gpu: bool,
        checks: bool,
        determinism_check_enabled: bool,
        read_kwargs: KwargsLike,
        write_kwargs: KwargsLike,
        logger: logging.Logger,
    ):
        super().__init__(
            competition_format=competition.format,
            prediction_directory_path=prediction_directory_path,
            determinism_check_enabled=determinism_check_enabled,
        )

        self.user_module = user_module
        self.runner_module = runner_module
        self.model_directory_path = model_directory_path
        self.force_first_train = force_first_train
        self.train_frequency = train_frequency
        self.round_number: RoundIdentifierType = round_number
        self.has_gpu = has_gpu
        self.checks = checks
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs
        self.logger = logger

        self.metrics = competition.metrics.list()

    def start(self):
        memory_before = get_process_memory()
        start = time.time()

        try:
            return super().start()
        finally:
            self.log(
                "duration - time=%s" % (
                    time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
                ),
                important=True,
            )

            memory_after = get_process_memory()
            memory_consumed = memory_after - memory_before

            self.log(
                'memory - before="%s" after="%s" consumed=%s' % (
                    format_bytes(memory_before),
                    format_bytes(memory_after),
                    f'"{format_bytes(memory_consumed)}"' if memory_consumed > 0 else "unknown"
                ),
                important=True,
            )

    def setup(self):
        tester.install_logger()
        monkey_patches.display_add()

    def initialize(self):
        self.log("running local test")
        self.log("internet access isn't restricted, no check will be done", important=True)
        self.log("")

        os.makedirs(self.model_directory_path, exist_ok=True)
        os.makedirs(self.prediction_directory_path, exist_ok=True)

        try:
            (
                self.embargo,
                self.number_of_features,
                self.keys,
                self.features,
                self.column_names,
                self.data_directory_path,
                self.data_paths,
            ) = download(
                round_number=self.round_number,
            )
        except (CrunchNotFoundException, MissingPhaseDataException):
            download_no_data_available()
            raise click.Abort()

        return (
            self.keys,
            False,
        )

    def start_timeseries(self) -> None:
        self.log(f"finding functions", important=True)
        self.train_function = ensure.is_function(self.user_module, "train", logger=self.logger)
        self.infer_function = ensure.is_function(self.user_module, "infer", logger=self.logger)

        self.log(f"loading data", important=True)
        self.x_train_path = self.data_paths[KnownData.X_TRAIN]
        self.y_train_path = self.data_paths[KnownData.Y_TRAIN]
        self.x_test_path = self.data_paths[KnownData.X_TEST]
        self.y_test_path = self.data_paths.get(KnownData.Y_TEST)
        self.example_prediction_path = self.data_paths[KnownData.EXAMPLE_PREDICTION]

        self.full_x: pandas.DataFrame = pandas.concat([
            read(self.x_train_path, kwargs=self.read_kwargs),
            read(self.x_test_path, kwargs=self.read_kwargs),
        ])

        if self.y_test_path:
            self.full_y: pandas.DataFrame = pandas.concat([
                read(self.y_train_path, kwargs=self.read_kwargs),
                read(self.y_test_path, kwargs=self.read_kwargs),
            ])
        else:
            self.full_y: pandas.DataFrame = read(self.y_train_path, kwargs=self.read_kwargs)

        for dataframe in [self.full_x, self.full_y]:
            dataframe.set_index(self.column_names.moon, drop=True, inplace=True)

        super().start_timeseries()

        if self.checks and not self.competition_format.unstructured:
            prediction: pandas.DataFrame = read(self.prediction_parquet_file_path)
            example_prediction: pandas.DataFrame = read(self.example_prediction_path)

            try:
                checker.run_via_api(
                    prediction,
                    example_prediction,
                    self.column_names,
                    self.logger,
                )

                self.log(f"prediction is valid", important=True)
            except checker.CheckError as error:
                cause = error.__cause__
                if not isinstance(cause, checker.CheckError):
                    self.logger.exception(
                        f"check failed - message=`{error}`",
                        exc_info=cause
                    )
                else:
                    self.log(f"check failed - message=`{error}`", error=True)

                return None

    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        target_column_names, prediction_column_names = Columns.from_model(self.column_names)

        default_values: KwargsLike = {
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
            self.log("call: train", important=True)
            x_train = self.filter_embargo(self.full_x, moon)
            y_train = self.filter_embargo(self.full_y, moon)

            smart_call(
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
            self.log("call: infer", important=True)
            x_test = self.filter_at(self.full_x, moon)

            prediction = smart_call(
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

    def start_unstructured(self) -> None:
        if self.runner_module is None:
            self.log("no runner is available for this competition", error=True)
            raise click.Abort()

        context = LocalRunnerContext(self)

        self.runner_module.run(
            context=context,
            data_directory_path=self.data_directory_path,
            model_directory_path=self.model_directory_path,
            prediction_directory_path=self.prediction_directory_path,
        )

        self.log(f"save prediction - path={self.prediction_directory_path}", important=True)

    def finalize(self):
        pass

    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ):
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
    def train_frequency(self):
        return self.runner.train_frequency

    @property
    def force_first_train(self):
        return self.runner.force_first_train

    @property
    def is_local(self):
        return True

    @property
    def is_submission_phase(self):
        return True

    @property
    def chain_height(self):
        return 1

    @property
    def has_model(self):
        return False

    @property
    def is_determinism_check_enabled(self):
        return self.runner.determinism_check_enabled

    def report_determinism(self, deterministic: bool):
        self.runner.deterministic = deterministic

    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ) -> Literal[True]:
        return self.runner.log(
            message,
            important=important,
            error=error,
        )

    def execute(
        self,
        *,
        command: str,
        parameters: Optional[KwargsLike] = None,
    ) -> None:
        self.log(f"executing - command={command}")

        user_module = LocalUserModule(self.runner)
        executor_context = LocalRunnerExecutorContext(self.runner)

        assert self.runner.runner_module
        handlers = self.runner.runner_module.execute(
            context=executor_context,
            module=user_module,
            data_directory_path=self.runner.data_directory_path,
            model_directory_path=self.runner.model_directory_path,
            prediction_directory_path=self.runner.prediction_directory_path,
        )

        handler = handlers.get(command)
        if handler is None:
            self.log(f"command not found: {command}", error=True)
            return None

        smart_call(
            handler,
            parameters or {},
        )


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
