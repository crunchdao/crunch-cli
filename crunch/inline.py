import logging
import sys
import typing

import click
import pandas

from . import api, command, constants, library, tester, utils


class _Inline:

    def __init__(self, module: typing.Any, model_directory: str, has_gpu=False):
        self.module = module
        self.model_directory = model_directory
        self.has_gpu = has_gpu

        print(f"loaded inline runner with module: {module}")

    def load_notebook(self, *args, **kwargs):
        return load(*args, **kwargs)

    def load_data(self, **kwargs) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                _,  # column names
                (
                    x_train_path,
                    y_train_path,
                    x_test_path,
                    _,  # y_test_path
                    _,  # example_prediction
                )
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
        read_kwargs={},
        write_kwargs={},
    ):
        tester.install_logger()

        try:
            library.scan(module=self.module)

            logging.warn('')

            return tester.run(
                self.module,
                self.model_directory,
                force_first_train,
                train_frequency,
                round_number,
                self.has_gpu,
                read_kwargs,
                write_kwargs,
            )
        except click.Abort as abort:
            logging.error(f"Aborted!")

            if raise_abort:
                raise abort

            return None

    def alpha_score(
        self,
        prediction: pandas.DataFrame
    ):
        from . import orthogonalization
        return orthogonalization.run(prediction)

    @property
    def is_inside_runner(self):
        from . import runner
        return runner.is_inside


def load(
    module_or_module_name: typing.Any = "__main__",
    model_directory=constants.DEFAULT_MODEL_DIRECTORY
):
    if isinstance(module_or_module_name, str):
        module = sys.modules[module_or_module_name]
    else:
        module = module_or_module_name

    return _Inline(module, model_directory)
