import json
import os
import sys
import time
import traceback
from functools import cached_property
from types import ModuleType
from typing import Any, List, Optional, Tuple, Union

import requests

from crunch.api import CompetitionFormat, DataReleaseSplit
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerExecutorContext, UserModule
from crunch.unstructured import LocalCodeLoader, RunnerModule
from crunch.utils import smart_call


class SandboxExecutor:

    def __init__(
        self,
        competition_name: str,
        competition_format: CompetitionFormat,
        # ---
        data_directory_path: str,
        main_file: str,
        code_directory: str,
        model_directory_path: str,
        prediction_directory_path: str,
        trace_path: str,
        state_file: str,
        ping_urls: List[str],
        # ---
        train: bool,
        loop_key: Union[int, str],
        embargo: int,
        number_of_features: int,
        gpu: bool,
        # ---
        fuse_pid: int,
        fuse_signal_number: int,
        # ---
        runner_dot_py_file_path: str,
        parameters: KwargsLike,
    ):
        self.competition_name = competition_name
        self.competition_format = competition_format

        self.data_directory_path = data_directory_path
        self.main_file = main_file
        self.code_directory = code_directory
        self.model_directory_path = model_directory_path
        self.prediction_directory_path = prediction_directory_path
        self.trace_path = trace_path
        self.state_file = state_file
        self.ping_urls = ping_urls

        self.train = train
        self.loop_key = loop_key
        self.embargo = embargo
        self.number_of_features = number_of_features
        self.gpu = gpu

        self.fuse_pid = fuse_pid
        self.fuse_signal_number = fuse_signal_number

        self.runner_dot_py_file_path = runner_dot_py_file_path
        self.parameters = parameters

    def signal_permission_fuse(self):
        first_file_path = next(iter(os.listdir(self.data_directory_path)), None)
        if not first_file_path:
            return  # no data?

        test_path = os.path.join(self.data_directory_path, first_file_path)

        os.kill(self.fuse_pid, self.fuse_signal_number)

        time.sleep(0.1)
        for _ in range(10):
            if not os.access(test_path, os.R_OK):
                break

            time.sleep(1)
            print(f"[debug] fuse not yet triggered - test_path=`{test_path}`", file=sys.stderr)
        else:
            print("fuse never triggered", file=sys.stderr)
            exit(1)

    def start(self):
        self._ping(self.ping_urls)

        with open(self.state_file, "r") as fd:
            self.state = json.load(fd)

        self.splits: List[DataReleaseSplit] = DataReleaseSplit.from_dict_array(self.state["splits"])

        try:
            if self.competition_format != CompetitionFormat.UNSTRUCTURED:
                raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")

            self._process_unstructured()
        except BaseException:
            self.write_trace(sys.exc_info())
            raise
        else:
            self.write_trace(None)

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
        assert self.runner_dot_py_file_path is not None

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

        command = str(self.loop_key)  # TODO Don't repurpose loop-key and use a dedicated property

        handler = handlers.get(command)
        if handler is None:
            raise ValueError(f"command `{command}` not found")

        smart_call(
            handler,
            self.parameters,
        )

    def write_trace(self, exc_info: Optional[Tuple[Any, ...]]):
        if exc_info is None:
            open(self.trace_path, "w").close()
            return

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
