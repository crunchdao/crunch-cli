import logging
import sys
import typing

import click
import pandas

from . import api, command, constants, orthogonalization, runner, utils


class _Inline:

    def __init__(self, module: typing.Any, model_directory: str, has_gpu=False):
        self.module = module
        self.model_directory = model_directory
        self.has_gpu = has_gpu

        print(f"loaded inline runner with module: {module}")

    def load_data(self, **kwargs) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # features
                _,  # column names
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

    def test(
        self,
        force_first_train=True,
        train_frequency=1,
        raise_abort=False,
        round_number="@current",
        no_checks=False,
        no_determinism_check=False,
        read_kwargs={},
        write_kwargs={},
    ):
        from . import library, tester

        _, project = api.Client.from_project()
        competition = project.competition.reload()

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
                competition.format,
                self.has_gpu,
                not no_checks,
                not no_determinism_check,
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
