import enum
import functools
import gc
import importlib
import importlib.util
import json
import os
import sys
import time
import traceback
import typing

import pandas
import requests

from .. import api, meta, unstructured, utils
from ..container import Columns, Features, GeneratorWrapper, StreamMessage
from .shared import split_streams
from .unstructured import RunnerExecutorContext, UserModule


class Reference:

    def __init__(self, initial_value):
        self.value = initial_value


class NamedFile(enum.Enum):

    X = "X"
    Y = "Y"

    def __repr__(self):
        return self.name


def ensure_function(module, name: str):
    if not hasattr(module, name):
        raise ValueError(f"no `{name}` function found")

    return getattr(module, name)


def ensure_tuple(input):
    if not isinstance(input, tuple):
        raise ValueError("result is not a tuple")

    if len(input) != 3:
        raise ValueError("result tuple must be of length 3")


def ensure_dataframe(input, name: str):
    if not isinstance(input, pandas.DataFrame):
        raise ValueError(f"{name} must be a dataframe")


@utils.timeit(["path"])
def read(path: str) -> pandas.DataFrame:
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


@utils.timeit(["path"])
def write(dataframe: pandas.DataFrame, path: str, index=False) -> None:
    if path.endswith(".parquet"):
        dataframe.to_parquet(path, index=index)
    else:
        dataframe.to_csv(path, index=index)


@utils.timeit([])
def ping(urls: typing.List[str]):
    for url in urls:
        try:
            requests.get(url)

            print(f"managed to have access to the internet: {url}", file=sys.stderr)
            exit(1)
        except requests.exceptions.RequestException:
            pass


class SandboxExecutor:

    def __init__(
        self,
        competition_name: str,
        competition_format: api.CompetitionFormat,
        # ---
        x_path: str,
        y_path: str,
        y_raw_path: str,
        data_directory_path: str,
        # ---
        main_file: str,
        code_directory: str,
        model_directory_path: str,
        prediction_path: str,
        trace_path: str,
        state_file: str,
        ping_urls: typing.List[str],
        # ---
        train: bool,
        loop_key: typing.Union[int, str],
        embargo: int,
        number_of_features: int,
        gpu: bool,
        # ---
        column_names: api.ColumnNames,
        # ---
        write_index: bool,
        # ---
        fuse_pid: int,
        fuse_signal_number: int,
        # ---
        runner_dot_py_file_path: str,
        parameters: dict,
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

    def load_data(self, trained: Reference):
        if self.competition_format.unstructured:
            return None, None, None

        full_x = read(self.x_path)

        if self.train:
            full_y = read(self.y_path)

            x_train = self.filter_train(full_x, NamedFile.X)
            y_train = self.filter_train(full_y, NamedFile.Y)

            del full_y
        else:
            x_train = None
            y_train = None

        x_test = self.filter_test(full_x, NamedFile.X)
        del full_x

        gc.collect()

        return x_train, y_train, x_test

    def _signal_permission_fuse(self):
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
        self.splits = api.DataReleaseSplit.from_dict_array(self.state["splits"])
        self.metrics = api.Metric.from_dict_array(self.state["metrics"], None)
        self.checks = api.Check.from_dict_array(self.state["checks"], None)
        self.features = Features(
            api.DataReleaseFeature.from_dict_array(self.state["features"]),
            self.state["default_feature_group"]
        )

        # keep local
        trained = Reference(False)
        x_train, y_train, x_test = self.load_data(trained)

        if not self.competition_format.unstructured:
            self._signal_permission_fuse()

        try:
            if self.competition_format == api.CompetitionFormat.UNSTRUCTURED:
                prediction = self.process_unstructured()
            else:
                module = self.load_module()

                print("[debug] user code loaded")

                train_function = ensure_function(module, "train")
                infer_function = ensure_function(module, "infer")

                if self.competition_format in [api.CompetitionFormat.TIMESERIES, api.CompetitionFormat.DAG]:
                    prediction = self.process_linear(
                        train_function,
                        infer_function,
                        x_train,
                        y_train,
                        x_test,
                        trained,
                    )

                elif self.competition_format == api.CompetitionFormat.STREAM:
                    prediction = self.process_async(
                        train_function,
                        infer_function,
                        x_train,
                        y_train,
                        x_test,
                    )

                elif self.competition_format == api.CompetitionFormat.SPATIAL:
                    prediction = self.process_spatial(
                        train_function,
                        infer_function,
                    )

                else:
                    raise ValueError(f"unsupported competition format: {self.competition_format}")
        except BaseException:
            self.write_trace(sys.exc_info())
            raise
        else:
            self.reset_trace()

        print(f"[debug] produced dataframe - len={len(prediction) if prediction is not None else None}")
        if prediction is not None:
            write(
                prediction,
                self.prediction_path,
                index=self.write_index
            )

    def load_module(self):
        from ..command.test import load_user_code

        main_file_path = os.path.join(self.code_directory, self.main_file)

        return load_user_code(main_file_path)

    def process_linear(
        self,
        train_function: callable,
        infer_function: callable,
        x_train: typing.Union[pandas.DataFrame, typing.Dict[str, pandas.DataFrame]],
        y_train: typing.Union[pandas.DataFrame, typing.Dict[str, pandas.DataFrame]],
        x_test: typing.Union[pandas.DataFrame, typing.Dict[str, pandas.DataFrame]],
        trained: Reference
    ):
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
            "moon": self.loop_key,
            "current_moon": self.loop_key,
            "embargo": self.embargo,
            "has_gpu": self.gpu,
            "has_trained": self.train,
            **self.features.to_parameter_variants(),
        }

        if self.train:
            utils.smart_call(
                train_function,
                default_values,
                {
                    "X_train": x_train,
                    "x_train": x_train,
                    "Y_train": y_train,
                    "y_train": y_train,
                },
                log=False
            )

            trained.value = True

        prediction = utils.smart_call(
            infer_function,
            default_values,
            {
                "X_test": x_test,
                "x_test": x_test,
            },
            log=False
        )

        ensure_dataframe(prediction, "prediction")
        return prediction

    def process_async(
        self,
        train_function: callable,
        infer_function: callable,
        x_train: pandas.DataFrame,
        y_train: pandas.DataFrame,
        x_test: pandas.DataFrame
    ):
        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "embargo": self.embargo,
            "has_gpu": self.gpu,
        }

        if self.train:
            streams = split_streams(x_train, self.column_names)

            utils.smart_call(
                train_function,
                default_values,
                {
                    "streams": streams,
                },
                log=False
            )

            return None
        else:
            side_column_name: str = self.column_names.side

            target_column_names = self.column_names.get_target_by_name(self.loop_key)
            assert target_column_names is not None, f"target not found: {self.loop_key}"

            stream_datas = [
                part[side_column_name]
                for part in utils.split_at_nans(x_test, side_column_name)
            ]

            time_meta_metrics = meta.filter_metrics(
                self.metrics,
                target_column_names.name,
                api.ScorerFunction.META__EXECUTION_TIME
            )

            predicteds, durations = [], []
            for index, stream_data in enumerate(stream_datas):
                print(f'call: infer ({index+ 1}/{len(stream_datas)})')

                wrapper = GeneratorWrapper(
                    iter(stream_data),
                    lambda stream: utils.smart_call(
                        infer_function,
                        default_values, {
                            "stream": stream,
                        }
                    ),
                    element_wrapper_factory=StreamMessage,
                )

                collected_values, collected_durations = wrapper.collect(len(stream_data))
                predicteds.extend(collected_values)

                if len(time_meta_metrics):
                    durations.extend(collected_durations)

            x_test.dropna(subset=[self.column_names.side], inplace=True)

            return pandas.DataFrame({
                self.column_names.moon: x_test[self.column_names.moon].values,
                self.column_names.id: x_test[self.column_names.id].values,
                self.column_names.output: pandas.Series(predicteds),
                **{
                    meta.to_column_name(metric, self.column_names.output): pandas.Series(durations).astype(int)
                    for metric in time_meta_metrics
                }
            })

    def process_spatial(
        self,
        train_function: callable,
        infer_function: callable,
    ):
        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "data_directory_path": self.data_directory_path,
            "embargo": self.embargo,
            "has_gpu": self.gpu,
        }

        if self.train:
            utils.smart_call(
                train_function,
                default_values,
                log=False
            )

            return None
        else:
            target_column_names = self.column_names.get_target_by_name(self.loop_key)
            assert target_column_names is not None, f"target not found: {self.loop_key}"

            data_file_path = os.path.join(
                self.data_directory_path,
                target_column_names.file_path
            ) if target_column_names.file_path else None

            prediction = utils.smart_call(
                infer_function,
                default_values,
                {
                    "data_file_path": data_file_path,
                    "target_name": target_column_names.name,
                }
            )

            ensure_dataframe(prediction, "prediction")
            return prediction

    def process_unstructured(self):
        loader = unstructured.LocalCodeLoader(self.runner_dot_py_file_path)
        runner_module = unstructured.RunnerModule.load(loader)
        assert runner_module is not None

        user_module = CloudExecutorUserModule(self)
        executor_context = CloudExecutorRunnerExecutorContext(self)

        handlers = runner_module.execute(
            executor_context,
            user_module,
            self.data_directory_path,
            self.model_directory_path,
        )

        command = self.loop_key  # TODO Don't repurpose loop-key and use a dedicated property

        handler = handlers.get(command)
        if handler is None:
            self.log(f"command not found: {command}", error=True)
            return None

        return utils.smart_call(
            handler,
            self.parameters,
        )

    def filter_train(self, dataframe: pandas.DataFrame, named_file: NamedFile):
        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            # TODO Use split's key with (index(moon) - embargo)
            dataframe = dataframe[dataframe[self.column_names.moon] < self.loop_key - self.embargo].copy()
            dataframe.reset_index(inplace=True, drop=True)

            return dataframe

        return self._filter_at_keys(
            api.DataReleaseSplitGroup.TRAIN,
            dataframe,
            named_file,
        )

    def filter_test(
        self,
        dataframe: pandas.DataFrame,
        named_file: NamedFile,
    ):
        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            dataframe = dataframe[dataframe[self.column_names.moon] == self.loop_key].copy()
            dataframe.reset_index(inplace=True, drop=True)

            return dataframe

        return self._filter_at_keys(
            api.DataReleaseSplitGroup.TEST,
            dataframe,
            named_file,
        )

    def _filter_at_keys(
        self,
        group: api.DataReleaseSplitGroup,
        input: typing.Union[pandas.DataFrame, typing.Dict[str, pandas.DataFrame]],
        named_file: NamedFile,
    ):
        keys = {
            split.key
            for split in self.splits
            if split.group == group
        }

        if self.competition_format == api.CompetitionFormat.DAG:
            dataframes = typing.cast(typing.Dict[str, pandas.DataFrame], input)

            return {
                key: value
                for key, value in dataframes.items()
                if key in keys
            }

        if self.competition_format == api.CompetitionFormat.STREAM:
            dataframe = typing.cast(pandas.DataFrame, input)
            target_column_name = self.column_names.get_target_by_name(self.loop_key)

            column_name = self.column_names.side
            if named_file == NamedFile.Y:
                column_name = self.column_names.input

            dataframe = dataframe[[
                self.column_names.moon,
                self.column_names.id,
                column_name,
            ]]

            if not self.train:
                dataframe = dataframe[dataframe[self.column_names.id] == target_column_name.name]

            dataframe = dataframe[dataframe[self.column_names.moon].isin(keys)]

            return dataframe.copy()

        raise ValueError(f"unsupported: {self.competition_format}")

    def reset_trace(self):
        open(self.trace_path, "w").close()

    def write_trace(self, exc_info):
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
        self.executor._signal_permission_fuse()


class CloudExecutorUserModule(UserModule):

    def __init__(self, executor: SandboxExecutor):
        self.executor = executor

    @functools.cached_property
    def module(self):
        return self.executor.load_module()
