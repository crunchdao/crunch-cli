import os
import sys
import traceback
from functools import cached_property
from time import sleep
from types import ModuleType
from typing import Any, List, Tuple

import requests

from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerExecutorContext, UserModule
from crunch.unstructured import LocalCodeLoader, RunnerModule
from crunch.utils import smart_call


class SandboxExecutor:

    def __init__(
        self,
        competition_name: str,
        # ---
        data_directory_path: str,
        main_file: str,
        code_directory: str,
        model_directory_path: str,
        prediction_directory_path: str,
        trace_path: str,
        ping_urls: List[str],
        # ---
        train: bool,
        gpu: bool,
        # ---
        fuse_pid: int,
        fuse_signal_number: int,
        # ---
        runner_dot_py_file_path: str,
        command: str,
        parameters: KwargsLike,
    ):
        self.competition_name = competition_name

        self.data_directory_path = data_directory_path
        self.main_file = main_file
        self.code_directory = code_directory
        self.model_directory_path = model_directory_path
        self.prediction_directory_path = prediction_directory_path
        self.trace_path = trace_path
        self.ping_urls = ping_urls

        self.train = train
        self.gpu = gpu

        self.fuse_pid = fuse_pid
        self.fuse_signal_number = fuse_signal_number

        self.runner_dot_py_file_path = runner_dot_py_file_path
        self.command = command
        self.parameters = parameters

    def signal_permission_fuse(self):
        if self.fuse_pid == 0:
            return  # fuse not installed

        first_file_path = next(iter(os.listdir(self.data_directory_path)), None)
        if not first_file_path:
            return  # no data?

        test_path = os.path.join(self.data_directory_path, first_file_path)

        os.kill(self.fuse_pid, self.fuse_signal_number)

        sleep(0.1)
        for _ in range(10):
            if not os.access(test_path, os.R_OK):
                break

            sleep(1)
            print(f"[debug] fuse not yet triggered - test_path=`{test_path}`", file=sys.stderr)
        else:
            print("fuse never triggered", file=sys.stderr)
            exit(1)

    def start(self):
        self._ping(self.ping_urls)

        try:
            self._process_unstructured()
        except BaseException:
            self.write_trace(sys.exc_info())
            raise

    def _ping(self, urls: List[str]):
        for url in urls:
            try:
                requests.get(url)

                print(f"managed to have access to the internet: {url}", file=sys.stderr)
                exit(1)
            except requests.exceptions.RequestException:
                pass

    def load_module(self) -> ModuleType:
        from crunch.command.test import load_user_code

        main_file_path = os.path.join(self.code_directory, self.main_file)

        return load_user_code(main_file_path)

    def _process_unstructured(self) -> None:
        loader = LocalCodeLoader(path=self.runner_dot_py_file_path)
        runner_module = RunnerModule.load(loader)
        assert runner_module is not None

        user_module = CloudExecutorUserModule(self)
        executor_context = CloudExecutorRunnerExecutorContext(self)

        handlers = runner_module.execute(
            context=executor_context,
            module=user_module,
            data_directory_path=self.data_directory_path,
            model_directory_path=self.model_directory_path,
            prediction_directory_path=self.prediction_directory_path,
        )

        handler = handlers.get(self.command)
        if handler is None:
            raise ValueError(f"command `{self.command}` not found")

        smart_call(
            handler,
            self.parameters,
        )

    def write_trace(self, exc_info: Tuple[Any, ...]):
        try:
            with open(self.trace_path, "w") as fd:
                traceback.print_exception(*exc_info, file=fd)
        except BaseException as ignored:
            print(f"ignored exception when reporting trace: {type(ignored)}({ignored})", file=sys.stderr)


class CloudExecutorRunnerExecutorContext(RunnerExecutorContext):

    def __init__(self, executor: SandboxExecutor):
        self.executor = executor

    @property
    def is_local(self):
        return False

    def trip_data_fuse(self):
        self.executor.signal_permission_fuse()


class CloudExecutorUserModule(UserModule):

    def __init__(self, executor: SandboxExecutor):
        self.executor = executor

    @property
    def module(self):
        return self._cached_module

    @cached_property
    def _cached_module(self):
        return self.executor.load_module()
