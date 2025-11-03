import gc
import json
import os
import sys
import time
import traceback
from functools import cached_property
from types import ModuleType
from typing import Any, Callable, List, Optional, Tuple, Union

import pandas
import requests

from crunch.api import Check, ColumnNames, CompetitionFormat, DataReleaseFeature, DataReleaseSplit, Metric
from crunch.container import Columns, Features
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerExecutorContext, UserModule
from crunch.unstructured import LocalCodeLoader, RunnerModule
from crunch.utils import smart_call, timeit


class SandboxExecutor:

    def __init__(
        self,
        competition_name: str,
        competition_format: CompetitionFormat,
        # ---
        x_path: str,
        y_path: str,
        y_raw_path: str,
        data_directory_path: str,
        # ---
        main_file: str,
        code_directory: str,
        model_directory_path: str,
        prediction_directory_path: str,
        prediction_path: str,
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
        column_names: ColumnNames,
        # ---
        write_index: bool,
        # ---
        fuse_pid: int,
        fuse_signal_number: int,
        # ---
        runner_dot_py_file_path: Optional[str],
        parameters: KwargsLike,
    ):
        self.competition_name = competition_name
        self.competition_format = competition_format

        self.x_path = x_path
        self.y_path = y_path
        self.y_raw_path = y_raw_path
        self.data_directory_path = data_directory_path

        self.main_file = main_file
        self.code_directory = code_directory
        self.model_directory_path = model_directory_path
        self.prediction_directory_path = prediction_directory_path
        self.prediction_path = prediction_path
        self.trace_path = trace_path
        self.state_file = state_file
        self.ping_urls = ping_urls

        self.train = train
        self.loop_key = loop_key
        self.embargo = embargo
        self.number_of_features = number_of_features
        self.gpu = gpu

        self.column_names = column_names

        self.write_index = write_index

        self.fuse_pid = fuse_pid
        self.fuse_signal_number = fuse_signal_number

        self.runner_dot_py_file_path = runner_dot_py_file_path
        self.parameters = parameters

    def signal_permission_fuse(self):
        if self.competition_format.unstructured:
            test_path = next(iter(os.listdir(self.data_directory_path)), None)
            if not test_path:
                return  # no data?
            test_path = os.path.join(self.data_directory_path, test_path)
        else:
            test_path = self.x_path

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
        ping(self.ping_urls)

        self.state = read(self.state_file)
        self.splits = DataReleaseSplit.from_dict_array(self.state["splits"])
        self.metrics = Metric.from_dict_array(self.state["metrics"], None)
        self.checks = Check.from_dict_array(self.state["checks"], None)
        self.features = Features(
            DataReleaseFeature.from_dict_array(self.state["features"]),
            self.state["default_feature_group"]
        )

        try:
            if self.competition_format == CompetitionFormat.UNSTRUCTURED:
                self._process_unstructured()

            elif self.competition_format == CompetitionFormat.TIMESERIES:
                self._process_timeseries()

            else:
                raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")
        except BaseException:
            self.write_trace(sys.exc_info())
            raise
        else:
            self.reset_trace()

    def load_module(self) -> ModuleType:
        from crunch.command.test import load_user_code

        main_file_path = os.path.join(self.code_directory, self.main_file)

        return load_user_code(main_file_path)

    def _process_timeseries(self) -> None:
        x_train, y_train, x_test = self._load_timeseries_data()

        if not self.competition_format.unstructured:
            self.signal_permission_fuse()

        module = self.load_module()

        print("[debug] user code loaded")

        train_function = ensure_function(module, "train")
        infer_function = ensure_function(module, "infer")

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
            "moon": self.loop_key,
            "current_moon": self.loop_key,
            "embargo": self.embargo,
            "has_gpu": self.gpu,
            "has_trained": self.train,
            **self.features.to_parameter_variants(),
        }

        if self.train:
            smart_call(
                train_function,
                default_values,
                {
                    "X_train": x_train,
                    "x_train": x_train,
                    "Y_train": y_train,
                    "y_train": y_train,
                },
                log=False,
            )

        prediction = smart_call(
            infer_function,
            default_values,
            {
                "X_test": x_test,
                "x_test": x_test,
            },
            log=False,
        )

        ensure_dataframe(prediction, "prediction")

        print(f"[debug] produced dataframe - len={len(prediction)}")
        write(
            prediction,
            self.prediction_path,
            index=self.write_index
        )

    def _load_timeseries_data(self):
        full_x = read(self.x_path)

        if self.train:
            full_y = read(self.y_path)

            x_train = self._filter_timeseries_train(full_x)
            y_train = self._filter_timeseries_train(full_y)

            del full_y
        else:
            x_train = None
            y_train = None

        x_test = self._filter_timeseries_test(full_x)
        del full_x

        gc.collect()

        return x_train, y_train, x_test

    def _filter_timeseries_train(
        self,
        dataframe: pandas.DataFrame,
    ) -> pandas.DataFrame:
        # TODO Use split's key with (index(moon) - embargo)
        dataframe = dataframe[dataframe[self.column_names.moon] < self.loop_key - self.embargo].copy()  # type: ignore
        dataframe.reset_index(inplace=True, drop=True)

        return dataframe

    def _filter_timeseries_test(
        self,
        dataframe: pandas.DataFrame,
    ) -> pandas.DataFrame:
        dataframe = dataframe[dataframe[self.column_names.moon] == self.loop_key].copy()
        dataframe.reset_index(inplace=True, drop=True)

        return dataframe

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

    def reset_trace(self):
        open(self.trace_path, "w").close()

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


def ensure_function(module: ModuleType, name: str) -> Callable[..., Any]:
    if not hasattr(module, name):
        raise ValueError(f"no `{name}` function found")

    function = getattr(module, name)
    if not callable(function):
        raise ValueError(f"`{name}` is not a function")

    return function


def ensure_tuple(input: Any):
    if not isinstance(input, tuple):
        raise ValueError("result is not a tuple")

    if len(input) != 3:  # type: ignore
        raise ValueError("result tuple must be of length 3")


def ensure_dataframe(input: Any, name: str):
    if not isinstance(input, pandas.DataFrame):
        raise ValueError(f"{name} must be a dataframe")


@timeit(["path"])
def read(
    path: Optional[str],
) -> Any:
    if path is None:
        return None

    if path.endswith(".parquet"):
        return pandas.read_parquet(path)

    if path.endswith(".pickle"):
        return pandas.read_pickle(path)

    if path.endswith(".json"):
        with open(path, "r") as fd:
            return json.load(fd)

    return pandas.read_csv(path)


@timeit(["path"])
def write(
    dataframe: pandas.DataFrame,
    path: str,
    index: bool = False,
) -> None:
    if path.endswith(".parquet"):
        dataframe.to_parquet(path, index=index)
    else:
        dataframe.to_csv(path, index=index)


@timeit([])
def ping(urls: List[str]):
    for url in urls:
        try:
            requests.get(url)

            print(f"managed to have access to the internet: {url}", file=sys.stderr)
            exit(1)
        except requests.exceptions.RequestException:
            pass
