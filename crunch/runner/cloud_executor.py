import datetime
import functools
import gc
import importlib
import inspect
import json
import logging
import os
import sys
import time
import traceback
import typing

import pandas
import requests

from .. import api, checker, orthogonalization, scoring
from ..orthogonalization import _runner as orthogonalization_runner
from ..container import Columns, Features


class Reference:

    def __init__(self, initial_value):
        self.value = initial_value


class undefined:
    pass


def timeit(params: list):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs.update(zip(
                func.__code__.co_varnames[:func.__code__.co_argcount],
                args
            ))

            start_time = time.perf_counter()
            try:
                return func(**kwargs)
            finally:
                end_time = time.perf_counter()
                total_time = end_time - start_time

                if params is not None:
                    arguments = ", ".join([
                        str(value) if name in params else "..."
                        for name, value in kwargs.items()
                    ])

                    print(f'[debug] {func.__name__}({arguments}) took {total_time:.4f} seconds', file=sys.stderr)
                else:
                    print(f'[debug] {func.__name__} took {total_time:.4f} seconds', file=sys.stderr)

        return wrapper

    return decorator


timeit_noarg = timeit(None)


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


@timeit(["path"])
def read(path: str, then_delete: bool) -> pandas.DataFrame:
    if path is None:
        return None

    try:
        if path.endswith(".parquet"):
            return pandas.read_parquet(path)

        if path.endswith(".pickle"):
            return pandas.read_pickle(path)

        if path.endswith(".json"):
            with open(path, "r") as fd:
                return json.load(fd)

        return pandas.read_csv(path)
    finally:
        if then_delete:
            delete(path)


def delete(path: str):
    if path is None:
        return None

    if os.path.exists(path):
        os.remove(path)


@timeit(["path"])
def write(dataframe: pandas.DataFrame, path: str) -> None:
    if path.endswith(".parquet"):
        dataframe.to_parquet(path, index=False)
    else:
        dataframe.to_csv(path, index=False)


@timeit([])
def ping(urls: typing.List[str]):
    for url in urls:
        try:
            requests.get(url)

            print(f"managed to have access to the internet: {url}", file=sys.stderr)
            exit(1)
        except requests.exceptions.RequestException as e:
            pass


def call(function: callable, default_values: dict, specific_values: dict):
    values = {
        **default_values,
        **specific_values
    }

    def error(message: str, level="debug"):
        print(f"[{level}] {function.__name__}: {message}", file=sys.stderr)

    arguments = {}
    for name, parameter in inspect.signature(function).parameters.items():
        name_str = str(parameter)
        if name_str.startswith("*"):
            error(f"unsupported parameter: {name_str}")
            continue

        if parameter.default != inspect.Parameter.empty:
            error(f"skip parameter with default value: {name}={parameter.default}")
            continue

        value = values.get(name, undefined)
        if value is undefined:
            error(f"unknown parameter: {name}")
            value = None

        error(f"set {name}={value.__class__.__name__}", level="trace")
        arguments[name] = value

    handler = timeit_noarg(function)
    return handler(**arguments)


class SandboxExecutor:

    def __init__(
        self,
        competition_name: str,
        competition_format: api.CompetitionFormat,
        # ---
        x_path: str,
        y_path: str,
        y_raw_path: str,
        orthogonalization_data_path: str,
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
        moon: int,
        embargo: int,
        number_of_features: int,
        gpu: bool,
        # ---
        column_names: api.ColumnNames
    ):
        self.competition_name = competition_name
        self.competition_format = competition_format

        self.x_path = x_path
        self.y_path = y_path
        self.y_raw_path = y_raw_path
        self.orthogonalization_data_path = orthogonalization_data_path

        self.main_file = main_file
        self.code_directory = code_directory
        self.model_directory_path = model_directory_path
        self.prediction_path = prediction_path
        self.trace_path = trace_path
        self.state_file = state_file
        self.ping_urls = ping_urls

        self.train = train
        self.moon = moon
        self.embargo = embargo
        self.number_of_features = number_of_features
        self.gpu = gpu

        self.column_names = column_names

    def start(self):
        ping(self.ping_urls)

        self.state = read(self.state_file, False)
        self.splits = api.DataReleaseSplit.from_dict_array(self.state["splits"])
        self.metrics = api.Metric.from_dict_array(self.state["metrics"], None)
        self.checks = api.Check.from_dict_array(self.state["checks"], None)
        self.features = Features(
            api.DataReleaseFeature.from_dict_array(self.state["features"]),
            self.state["default_feature_group"]
        )

        full_x = read(self.x_path, True)

        # keep local
        trained = Reference(False)

        if self.train:
            full_y = read(self.y_path, True)

            x_train = self.filter_train(full_x)
            y_train = self.filter_train(full_y)

            del full_y

            if self.orthogonalization_data_path:
                full_y_raw = None
                if self.y_raw_path:
                    full_y_raw = read(self.y_raw_path, True)
                    y_raw = self.filter_train(full_y_raw)

                self.setup_orthogonalization(y_train, y_raw, trained)

        delete(self.y_path)
        delete(self.y_raw_path)
        delete(self.y_raw_path)
        delete(self.orthogonalization_data_path)

        x_test = self.filter_test(full_x)
        del full_x

        gc.collect()

        try:
            spec = importlib.util.spec_from_file_location(
                "user_code",
                os.path.join(self.code_directory, self.main_file)
            )

            module = importlib.util.module_from_spec(spec)

            sys.path.insert(0, self.code_directory)
            spec.loader.exec_module(module)

            train_function = ensure_function(module, "train")
            infer_function = ensure_function(module, "infer")
            
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
                "moon": self.moon,
                "current_moon": self.moon,
                "embargo": self.embargo,
                "has_gpu": self.gpu,
                "has_trained": self.train,
                **self.features.to_parameter_variants(),
            }

            if self.train:
                call(train_function, default_values, {
                    "X_train": x_train,
                    "x_train": x_train,
                    "Y_train": y_train,
                    "y_train": y_train,
                })

                trained.value = True
                if self.orthogonalization_data_path:
                    orthogonalization_runner.restore()

            prediction = call(infer_function, default_values, {
                "X_test": x_test,
                "x_test": x_test,
            })

            ensure_dataframe(prediction, "prediction")
        except BaseException:
            self.write_trace(sys.exc_info())
            raise
        else:
            self.reset_trace()

        write(prediction, self.prediction_path)

    def filter_train(self, dataframe: pandas.DataFrame):
        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            # TODO Use split's key with (index(moon) - embargo)
            dataframe = dataframe[dataframe[self.column_names.moon] < self.moon - self.embargo].copy()
            dataframe.reset_index(inplace=True, drop=True)

            return dataframe

        if self.competition_format == api.CompetitionFormat.DAG:
            dataframe: dict

            keys = [
                split.key
                for split in self.splits
                if split.group == api.DataReleaseSplitGroup.TRAIN
            ]

            return {
                key: value
                for key, value in dataframe.items()
                if key in keys
            }

        raise ValueError(f"unsupported: {self.competition_format}")

    def filter_test(self, dataframe: pandas.DataFrame):
        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            dataframe = dataframe[dataframe[self.column_names.moon] == self.moon].copy()
            dataframe.reset_index(inplace=True, drop=True)

            return dataframe

        if self.competition_format == api.CompetitionFormat.DAG:
            dataframe: dict

            keys = [
                split.key
                for split in self.splits
                if split.group == api.DataReleaseSplitGroup.TEST
            ]

            return {
                key: value
                for key, value in dataframe.items()
                if key in keys
            }

        raise ValueError(f"unsupported: {self.competition_format}")

    def setup_orthogonalization(
        self,
        y_train: pandas.DataFrame,
        y_raw: typing.Optional[pandas.DataFrame],
        trained: Reference
    ):
        REMOVED_CHECKS = [
            api.CheckFunction.CONSTANTS,
            api.CheckFunction.MOONS,
        ]

        full_orthogonalization_data = read(self.orthogonalization_data_path, True) if self.train else None

        split_keys = y_train[self.column_names.moon].unique()
        orthogonalization_data = {
            key: value
            for key, value in full_orthogonalization_data.items()
            if key in split_keys
        }
        del full_orthogonalization_data

        logger = logging.getLogger("orthogonalization")
        logger.setLevel(logging.WARNING)

        metric_by_name = {
            metric.name: metric
            for metric in self.metrics
        }

        def orthogonalize(prediction: pandas.DataFrame):
            if trained.value:
                raise ValueError("orthogonalize not available anymore")

            example_prediction = y_train[[self.column_names.moon, self.column_names.id]].copy()
            for prediction_column_name in self.column_names.outputs:
                example_prediction[prediction_column_name] = 0

            checker.run(
                [
                    check
                    for check in self.checks
                    if check.function not in REMOVED_CHECKS
                ],
                prediction,
                example_prediction,
                self.column_names,
                self.competition_format,
                logger
            )

            del example_prediction

            user_moons = set(prediction[self.column_names.moon].unique())
            y_keys = [
                key
                for key in split_keys
                if key in user_moons
            ]

            y = y_train[y_train[self.column_names.moon].isin(user_moons)].copy()

            if not len(y):
                return []

            orthogonalized = orthogonalization.process(
                self.competition_name,
                prediction,
                orthogonalization_data,
                self.column_names
            )

            result = scoring.score(
                self.competition_format,
                logger,
                y if y_raw is None else y_raw,
                orthogonalized,
                self.column_names,
                self.metrics,
                y_keys,
            )

            # TODO This mimic API behavior, a better system is required
            scores = []
            for metric_name, scored in result.items():
                metric = metric_by_name[metric_name]
                score = api.Score(
                    None,
                    {
                        "id": 0,
                        "success": True,
                        "metric": metric._attrs,
                        "value": scored.value,
                        "details": [
                            {
                                "key": detail.key,
                                "value": detail.value,
                            }
                            for detail in scored.details
                        ],
                        "createdAt": datetime.datetime.now().isoformat(),
                    }
                )

                scores.append(score)

            return scores

        orthogonalization_runner.set(orthogonalize)

    def reset_trace(self):
        open(self.trace_path, "w").close()

    def write_trace(self, exc_info):
        try:
            with open(self.trace_path, "w") as fd:
                traceback.print_exception(*exc_info, file=fd)
        except BaseException as ignored:
            print(f"ignored exception when reporting trace: {type(ignored)}({ignored})", file=sys.stderr)
