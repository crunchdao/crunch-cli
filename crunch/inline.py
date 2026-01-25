import json
import logging
import os
import sys
import urllib.parse
from functools import cached_property
from textwrap import dedent
from types import ModuleType
from typing import Any, Optional, Tuple, Union

import click
import pandas
import psutil

import crunch.tester as tester
from crunch.__version__ import __version__
from crunch.api import ApiException, Client, CompetitionFormat, CrunchNotFoundException, KnownData, MissingPhaseDataException, RoundIdentifierType
from crunch.command.convert import convert
from crunch.command.download import download, download_no_data_available
from crunch.command.push import push
from crunch.constants import DEFAULT_MAIN_FILE_PATH, DEFAULT_MODEL_DIRECTORY, DOT_PREDICTION_DIRECTORY
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

    def submit(
        self,
        message: Optional[str] = None,
        model_directory_relative_path: str = "",
        include_installed_packages_version: bool = False,
        notebook_file_name: str = "notebook.ipynb",
        main_file_name: str = DEFAULT_MAIN_FILE_PATH,
    ):
        if message is None:
            message = input("Message: ")

        if not model_directory_relative_path or os.path.realpath(model_directory_relative_path) == os.path.realpath("."):
            model_directory_relative_path = DEFAULT_MODEL_DIRECTORY

        try:
            from IPython.display import Markdown, display  # type: ignore
        except ImportError as error:
            print(f"submit: could not import ipython, are you running in a notebook?", file=sys.stderr)
            print(f"submit: catched error: {error}", file=sys.stderr)
            return

        try:
            from google.colab import _message  # type: ignore
            response = _message.blocking_request('get_ipynb', request='', timeout_sec=5)  # type: ignore

            if response is None:
                raise NotImplementedError(f"google.colab._message.blocking_request did not answered")

            error = response.get("error")  # type: ignore
            if error is not None:
                raise NotImplementedError(f"{error.get('type')}: {error.get('description')}")  # type: ignore

            ipynb = response.get("ipynb")  # type: ignore
            if ipynb is None:
                raise NotImplementedError(f"missing ipynb, available keys are: {list(response.keys())}")  # type: ignore

            if ipynb.get("cells") is None:  # type: ignore
                raise NotImplementedError(f"missing cells, available keys are: {list(ipynb.keys())}")  # type: ignore
        except (ImportError, NotImplementedError) as error:
            encoded_message = urllib.parse.quote_plus(message)

            display(Markdown(dedent(f"""
                ---

                Your work could not be submitted automatically, please do so manually:
                1. Download your Notebook from Colab
                2. Upload it to the platform
                3. Create a run to validate it

                ### >> [https://hub.crunchdao.com/competitions/{self._competition.name}/submit/notebook](https://hub.crunchdao.com/competitions/{self._competition.name}/submit/notebook?message={encoded_message})

                <img alt="Download and Submit Notebook" src=https://raw.githubusercontent.com/crunchdao/competitions/refs/heads/master/documentation/animations/download-and-submit-notebook.gif height="600px" />

                <br />
                <small>Error preventing submit: <code>{error}</code></small>
            """)))
            return

        files_before = set(os.listdir("."))

        try:
            return self._do_submit(
                ipynb=ipynb,
                message=message,
                model_directory_relative_path=model_directory_relative_path,
                include_installed_packages_version=include_installed_packages_version,
                notebook_file_name=notebook_file_name,
                main_file_name=main_file_name,
            )
        except click.Abort:
            print("aborted", file=sys.stderr)
        finally:
            files_after = set(os.listdir("."))
            new_files = files_after - files_before

            for file in new_files:
                try:
                    os.unlink(file)
                except FileNotFoundError:
                    pass

    def _do_submit(
        self,
        ipynb: Any,
        message: str,
        model_directory_relative_path: str,
        include_installed_packages_version: bool,
        notebook_file_name: str,
        main_file_name: str,
    ):
        from IPython.display import Markdown, display  # type: ignore

        with open(notebook_file_name, "w") as fd:
            json.dump(ipynb, fd)

        try:
            convert(
                notebook_file_path=notebook_file_name,
                python_file_path=main_file_name,
                write_requirements=True,
                write_embedded_files=True,
                no_freeze=True,  # will be frozen on push
                override=True,
            )
        except SystemExit as error:
            if error.code != 0:
                print("conversion failed", file=sys.stderr)
                return

        try:
            submission = push(
                message=message,
                main_file_path=main_file_name,
                model_directory_relative_path=model_directory_relative_path,
                include_installed_packages_version=include_installed_packages_version,
                no_afterword=True,
                dry=False,
            )
        except ApiException as error:
            print("\n---")
            error.print_helper()
            return

        project = submission.project
        display(Markdown(dedent(f"""
            ---

            Next step is to run your submission in the cloud:

            ### >> https://hub.crunchdao.com/competitions/{self._competition.name}/models/{project.user.login}/{project.name}/runs/create?submissionNumber={submission.number}

            <img alt="Run in the Cloud" src=https://raw.githubusercontent.com/crunchdao/competitions/refs/heads/master/documentation/animations/create-run.gif height="600px" />
        """)))

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
