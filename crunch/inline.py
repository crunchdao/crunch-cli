import typing
import pandas
import os
import sys

from . import utils, ensure, constants
from . import command, tester


class _Inline:

    def __init__(self, module: typing.Any, model_directory: str):
        self.module = module
        self.model_directory = model_directory

        self.session = utils.CustomSession(
            os.environ["WEB_BASE_URL"],
            os.environ["API_BASE_URL"],
            bool(os.environ.get("DEBUG", "False")),
        )

        print(f"loaded inline runner with module: {module}")
    
    def load_data(self) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
        (
            _,
            _,
            x_train_path,
            y_train_path,
            x_test_path
        ) = command.download(self.session)

        x_train = utils.read(x_train_path)
        y_train = utils.read(y_train_path)
        x_test = utils.read(x_test_path)

        return x_train, y_train, x_test

    def test(self, force_first_train=True, train_frequency=1):
        tester.run(
            self.module,
            self.session,
            self.model_directory,
            force_first_train,
            train_frequency,
        )


def load(module_or_module_name: typing.Any, model_directory=constants.DEFAULT_MODEL_DIRECTORY):
    if isinstance(module_or_module_name, str):
        module = sys.modules[module_or_module_name]
    else:
        module = module_or_module_name
    
    return _Inline(module, model_directory)
