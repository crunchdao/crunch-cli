import logging
import sys
from functools import cached_property
from types import ModuleType
from typing import Optional, Tuple, Union

import click
import pandas
import psutil

import crunch.tester as tester
from crunch.__version__ import __version__
from crunch.api import Client, CompetitionFormat, CrunchNotFoundException, KnownData, MissingPhaseDataException, RoundIdentifierType
from crunch.command.download import download, download_no_data_available
from crunch.constants import DEFAULT_MODEL_DIRECTORY, DOT_PREDICTION_DIRECTORY
from crunch.runner import is_inside
from crunch.runner.types import KwargsLike
from crunch.unstructured import RunnerModule, deduce_code_loader
from crunch.utils import read


class _Inline:

    def __init__(
        self,
        *,
        user_module: ModuleType,
        model_directory_path: str,
        logger: logging.Logger,
        has_gpu: bool = False,
    ):
        self.user_module = user_module
        self.model_directory_path = model_directory_path
        self.logger = logger
        self.has_gpu = has_gpu

        print(f"loaded inline runner with module: {user_module}")

        from . import is_inside_runner
        if is_inside_runner:
            print(f"[warning] loading the inliner in the cloud runner is not supported, this will raise an error soon", file=sys.stderr)

        print()

        version = __version__
        print(f"cli version: {version}")

        available_ram = psutil.virtual_memory().total / (1024 ** 3)
        print(f"available ram: {available_ram:.2f} gb")

        cpu_count = psutil.cpu_count()
        print(f"available cpu: {cpu_count} core")

        print(f"----")

    @cached_property
    def _competition(self):
        _, project = Client.from_project()
        competition = project.competition.reload()

        return competition

    def load_data(
        self,
        round_number: RoundIdentifierType = "@current",
        force: bool = False,
        **kwargs: KwargsLike,
    ) -> Tuple[Optional[pandas.DataFrame], Optional[pandas.DataFrame], Optional[pandas.DataFrame]]:
        if self._competition.format == CompetitionFormat.STREAM:
            self.load_streams()

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                _,  # column_names
                data_directory_path,
                data_paths,
            ) = download(
                round_number=round_number,
                force=force,
            )
        except (CrunchNotFoundException, MissingPhaseDataException):
            download_no_data_available()
            raise click.Abort()

        competition_format = self._competition.format
        if competition_format == CompetitionFormat.TIMESERIES:
            x_train_path = data_paths[KnownData.X_TRAIN]
            y_train_path = data_paths[KnownData.Y_TRAIN]
            x_test_path = data_paths[KnownData.X_TEST]

            x_train = read(x_train_path, kwargs=kwargs)
            y_train = read(y_train_path, kwargs=kwargs)
            x_test = read(x_test_path, kwargs=kwargs)

            return x_train, y_train, x_test

        elif competition_format == CompetitionFormat.UNSTRUCTURED:
            module = self._runner_module
            if module is None or module.get_load_data_function(ensure=False) is None:
                self.logger.info("Please follow the competition instructions to load the data.")
                return None, None, None

            return module.load_data(
                data_directory_path=data_directory_path,
                logger=self.logger,
            )

        else:
            raise NotImplementedError(f"{competition_format.name} competition format is not supported anymore")

    def load_streams(
        self,
        **kwargs: KwargsLike,
    ) -> None:
        raise NotImplementedError("STREAM competition format is not supported anymore")

    def test(
        self,
        force_first_train: bool = True,
        train_frequency: int = 1,
        raise_abort: bool = False,
        round_number: RoundIdentifierType = "@current",
        no_checks: bool = False,
        no_determinism_check: Optional[bool] = None,
        read_kwargs: KwargsLike = {},
        write_kwargs: KwargsLike = {},
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
                self.model_directory_path,
                DOT_PREDICTION_DIRECTORY,
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
        return is_inside

    @cached_property
    def _runner_module(self):
        loader = deduce_code_loader(
            competition_name=self._competition.name,
            file_name="runner",
        )

        return RunnerModule.load(loader)

    def __getattr__(self, key: str):
        import crunch

        return getattr(crunch, key)


def load(
    module_name_or_module: Union[ModuleType, str] = "__main__",
    model_directory_path: str = DEFAULT_MODEL_DIRECTORY,
):
    if isinstance(module_name_or_module, str):
        module = sys.modules[module_name_or_module]
    else:
        module = module_name_or_module

    logger = tester.install_logger()
    return _Inline(
        user_module=module,
        model_directory_path=model_directory_path,
        logger=logger,
    )
