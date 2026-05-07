import logging
import os
import time
from datetime import datetime
from typing import Any, Literal, Optional

import click

import crunch.monkey_patches as monkey_patches
import crunch.tester as tester
from crunch.api import Competition, CrunchNotFoundException, MissingPhaseDataException, RoundIdentifierType
from crunch.command import download, download_no_data_available
from crunch.external.humanfriendly import format_size
from crunch.runner.runner import Runner
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerContext, RunnerExecutorContext, UserModule
from crunch.unstructured import RunnerModule
from crunch.utils import get_process_memory, smart_call


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
        determinism_check_enabled: bool,
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
        self.logger = logger

        self.started_at = datetime.now()

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
                    format_size(memory_before),
                    format_size(memory_after),
                    f'"{format_size(memory_consumed)}"' if memory_consumed > 0 else "unknown"
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


class LocalRunnerContext(RunnerContext):

    def __init__(self, runner: LocalRunner):
        self.runner = runner

    @property
    def started_at(self) -> datetime:
        return self.runner.started_at

    @property
    def timeout(self):
        return None

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
