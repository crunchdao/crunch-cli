import functools
import logging
import sys
import typing

import click
import pandas
import psutil

from . import (__version__, api, command, constants, container, unstructured, runner,
               tester, utils)

LoadedData = typing.Union[
    pandas.DataFrame,
    typing.Dict[str, pandas.DataFrame]
]

Streams = typing.List[typing.Iterable[container.StreamMessage]]


class _Inline:

    def __init__(
        self,
        user_module: typing.Any,
        model_directory: str,
        logger: logging.Logger,
        has_gpu=False,
    ):
        self.user_module = user_module
        self.model_directory = model_directory
        self.logger = logger
        self.has_gpu = has_gpu

        print(f"loaded inline runner with module: {user_module}")

        from . import is_inside_runner
        if is_inside_runner:
            print(f"[warning] loading the inliner in the cloud runner is not supported, this will raise an error soon", file=sys.stderr)

        print()

        version = __version__.__version__
        print(f"cli version: {version}")

        available_ram = psutil.virtual_memory().total / (1024 ** 3)
        print(f"available ram: {available_ram:.2f} gb")

        cpu_count = psutil.cpu_count()
        print(f"available cpu: {cpu_count} core")

        print(f"----")

    @functools.cached_property
    def _competition(self):
        _, project = api.Client.from_project()
        competition = project.competition.reload()

        return competition

    def load_data(
        self,
        round_number="@current",
        force=False,
        **kwargs,
    ) -> typing.Tuple[LoadedData, LoadedData, LoadedData]:
        if self._competition.format == api.CompetitionFormat.STREAM:
            self.logger.error(f"Please call `.load_streams()` instead.")
            return None, None, None

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                _,  # column_names
                data_directory_path,
                data_paths,
            ) = command.download(
                round_number=round_number,
                force=force,
            )
        except (api.CrunchNotFoundException, api.MissingPhaseDataException):
            command.download_no_data_available()
            raise click.Abort()

        if self._competition.format.unstructured:
            module = self._runner_module
            if module is None or module.get_load_data_function(ensure=False) is None:
                self.logger.info("Please follow the competition instructions to load the data.")
                return None, None, None

            return module.load_data(
                data_directory_path=data_directory_path,
                logger=self.logger,
            )

        x_train_path = data_paths.get(api.KnownData.X_TRAIN)
        y_train_path = data_paths.get(api.KnownData.Y_TRAIN)
        x_test_path = data_paths.get(api.KnownData.X_TEST)

        x_train = utils.read(x_train_path, kwargs=kwargs)
        y_train = utils.read(y_train_path, kwargs=kwargs)
        x_test = utils.read(x_test_path, kwargs=kwargs)

        return x_train, y_train, x_test

    def load_streams(
        self,
        round_number="@current",
        force=False,
        **kwargs,
    ) -> typing.Tuple[Streams, Streams]:
        if self._competition.format.unstructured:
            self.logger.error(f"Please follow the competition instructions to load the data.")
            return None, None

        if self._competition.format != api.CompetitionFormat.STREAM:
            self.logger.error(f"Please call `.load_data()` instead.")
            return None, None

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                column_names,
                _,  # data_directory_path,
                data_paths,
            ) = command.download(
                round_number=round_number,
                force=force,
            )

            x_train_path = data_paths.get(api.KnownData.X_TRAIN)
            x_test_path = data_paths.get(api.KnownData.X_TEST)
        except (api.CrunchNotFoundException, api.MissingPhaseDataException):
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

        competition = self._competition

        try:
            library.scan(
                module=self.user_module,
                logger=self.logger,
            )
            self.logger.warning('')

            return tester.run(
                self.user_module,
                self._runner_module,
                self.model_directory,
                force_first_train,
                train_frequency,
                round_number,
                competition,
                self.has_gpu,
                not no_checks,
                no_determinism_check,
                read_kwargs,
                write_kwargs,
            )
        except KeyboardInterrupt:
            self.logger.error(f"Cancelled!")
        except click.Abort as abort:
            self.logger.error(f"Aborted!")

            if raise_abort:
                raise abort

        return None

    @property
    def is_inside_runner(self):
        return runner.is_inside

    @functools.cached_property
    def _runner_module(self):
        loader = unstructured.deduce_code_loader(
            self._competition.name,
            "runner",
        )

        return unstructured.RunnerModule.load(loader)

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

    logger = tester.install_logger()
    return _Inline(module, model_directory, logger)
