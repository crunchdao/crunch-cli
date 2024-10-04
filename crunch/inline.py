import functools
import logging
import sys
import typing

import click
import pandas

from . import (api, command, constants, container, orthogonalization, runner,
               utils)

LoadedData = typing.Union[
    pandas.DataFrame,
    typing.Dict[str, pandas.DataFrame]
]

Streams = typing.List[typing.Iterable[container.StreamMessage]]


class _Inline:

    def __init__(self, module: typing.Any, model_directory: str, has_gpu=False):
        self.module = module
        self.model_directory = model_directory
        self.has_gpu = has_gpu

        print(f"loaded inline runner with module: {module}")

    @functools.cached_property
    def _competition_format(self):
        _, project = api.Client.from_project()
        competition = project.competition.reload()

        return competition.format

    def load_data(self, **kwargs) -> typing.Tuple[LoadedData, LoadedData, LoadedData]:
        if self._competition_format == api.CompetitionFormat.STREAM:
            logging.error(f"Please call `.load_streams()` instead.")
            return None, None, None

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                _,  # column_names
                (
                    x_train_path,
                    y_train_path,
                    x_test_path,
                    _,  # y_test_path
                    _,  # example_prediction
                ),
            ) = command.download()
        except api.CrunchNotFoundException:
            command.download_no_data_available()
            raise click.Abort()

        x_train = utils.read(x_train_path, kwargs=kwargs)
        y_train = utils.read(y_train_path, kwargs=kwargs)
        x_test = utils.read(x_test_path, kwargs=kwargs)

        return x_train, y_train, x_test

    def load_streams(self, **kwargs) -> typing.Tuple[Streams, Streams]:
        if self._competition_format != api.CompetitionFormat.STREAM:
            logging.error(f"Please call `.load_data()` instead.")
            return None, None

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                column_names,
                (
                    x_train_path,
                    _,  # y_train_path
                    x_test_path,
                    _,  # y_test_path
                    _,  # example_prediction
                ),
            ) = command.download()
        except api.CrunchNotFoundException:
            command.download_no_data_available()
            raise click.Abort()

        x_train = utils.read(x_train_path, kwargs=kwargs)
        x_test = utils.read(x_test_path, kwargs=kwargs)

        def as_iterators(dataframe: pandas.DataFrame):
            column_name = typing.cast(str, column_names.side)

            return [
                container.CallableIterable.from_dataframe(part, column_name, container.StreamMessage)
                for _, group in dataframe.groupby(column_names.id)
                for part in utils.split_at_nans(group, column_name)
            ]

        x_train = as_iterators(x_train)
        x_test = as_iterators(x_test)

        return x_train, x_test

    def test(
        self,
        force_first_train=True,
        train_frequency=1,
        raise_abort=False,
        round_number="@current",
        no_checks=False,
        no_determinism_check: typing.Optional[bool] = None,
        read_kwargs={},
        write_kwargs={},
    ):
        from . import library, tester

        competition_format = self._competition_format

        tester.install_logger()

        try:
            library.scan(module=self.module)
            logging.warning('')

            return tester.run(
                self.module,
                self.model_directory,
                force_first_train,
                train_frequency,
                round_number,
                competition_format,
                self.has_gpu,
                not no_checks,
                no_determinism_check,
                read_kwargs,
                write_kwargs,
            )
        except KeyboardInterrupt:
            logging.error(f"Cancelled!")
        except click.Abort as abort:
            logging.error(f"Aborted!")

            if raise_abort:
                raise abort

        return None

    def alpha_score(
        self,
        prediction: pandas.DataFrame,
        as_dataframe=True,
        max_retry=orthogonalization.DEFAULT_MAX_RETRY,
        timeout=orthogonalization.DEFAULT_TIMEOUT,
    ):
        return orthogonalization.run(
            prediction,
            as_dataframe,
            max_retry,
            timeout,
        )

    @property
    def is_inside_runner(self):
        return runner.is_inside

    def __getattr__(self, key):
        import crunch

        return getattr(crunch, key)


def load(
    module_or_module_name: typing.Any = "__main__",
    model_directory=constants.DEFAULT_MODEL_DIRECTORY
):
    if isinstance(module_or_module_name, str):
        module = sys.modules[module_or_module_name]
    else:
        module = module_or_module_name

    return _Inline(module, model_directory)
