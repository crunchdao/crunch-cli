import datetime
import gc
import importlib
import json
import logging
import os
import sys
import enum
import traceback
import typing
import importlib.util

import pandas
import requests

from .. import api, checker, container, orthogonalization, scoring, utils
from ..container import Columns, Features, CallableIterable, GeneratorWrapper
from ..orthogonalization import _runner as orthogonalization_runner


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


@utils.timeit(["path"])
def write(dataframe: pandas.DataFrame, path: str) -> None:
    if path.endswith(".parquet"):
        dataframe.to_parquet(path, index=False)
    else:
        dataframe.to_csv(path, index=False)


@utils.timeit([])
def ping(urls: typing.List[str]):
    for url in urls:
        try:
            requests.get(url)

            print(f"managed to have access to the internet: {url}", file=sys.stderr)
            # exit(1)
        except requests.exceptions.RequestException as e:
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
        loop_key: typing.Union[int, str],
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
        self.loop_key = loop_key
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

            x_train = self.filter_train(full_x, NamedFile.X)
            y_train = self.filter_train(full_y, NamedFile.Y)

            del full_y

            if self.orthogonalization_data_path:
                full_y_raw = None
                if self.y_raw_path:
                    full_y_raw = read(self.y_raw_path, True)
                    y_raw = self.filter_train(full_y_raw, NamedFile.Y)

                self.setup_orthogonalization(y_train, y_raw, trained)
        else:
            x_train = None
            y_train = None

        delete(self.y_path)
        delete(self.y_raw_path)
        delete(self.y_raw_path)
        delete(self.orthogonalization_data_path)

        x_test = self.filter_test(full_x, NamedFile.X)
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

            if self.competition_format in [api.CompetitionFormat.TIMESERIES, api.CompetitionFormat.DAG]:
                prediction = self.process_linear(
                    train_function,
                    infer_function,
                    x_train,
                    y_train,
                    x_test,
                    trained
                )

            elif self.competition_format == api.CompetitionFormat.STREAM:
                prediction = self.process_async(
                    train_function,
                    infer_function,
                    x_train,
                    y_train,
                    x_test
                )

            else:
                raise ValueError(f"unsupported competition format: {self.competition_format}")
        except BaseException:
            self.write_trace(sys.exc_info())
            raise
        else:
            self.reset_trace()

        produce_nothing = self.train and self.competition_format == api.CompetitionFormat.STREAM
        if not produce_nothing:
            write(prediction, self.prediction_path)

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
            if self.orthogonalization_data_path:
                orthogonalization_runner.restore()

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
        target_column_name = self.column_names.get_target_by_name(self.loop_key)

        default_values = {
            "number_of_features": self.number_of_features,
            "model_directory_path": self.model_directory_path,
            "stream_name": self.loop_key,
            "embargo": self.embargo,
            "has_gpu": self.gpu,
            "has_trained": self.train,
            "horizon": 2,  # TODO load from competition
            **self.features.to_parameter_variants(),
        }

        side_column_name: str = self.column_names.side
        if self.train:
            streams = [
                CallableIterable.from_dataframe(part, side_column_name)
                for part in utils.split_at_nans(x_train, side_column_name)
            ]

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
            stream_datas = [
                part[side_column_name]
                for part in utils.split_at_nans(x_test, side_column_name)
            ]

            predicteds = []
            for index, stream_data in enumerate(stream_datas):
                logging.warning(f'call: infer ({index+ 1}/{len(stream_datas)})')

                wrapper = GeneratorWrapper(
                    iter(stream_data),
                    lambda stream: utils.smart_call(
                        infer_function,
                        default_values, {
                            "stream": stream,
                        }
                    )
                )

                collecteds = wrapper.collect(len(stream_data))
                predicteds.extend(collecteds)

            x_test.dropna(subset=[self.column_names.side], inplace=True)

            return pandas.DataFrame({
                self.column_names.moon: x_test[self.column_names.moon],
                self.column_names.id: x_test[self.column_names.id],
                self.column_names.output: predicteds,
            })

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
        keys = { split.key for split in self.splits if split.group == group }

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
