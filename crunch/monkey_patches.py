import functools
import os
import sys

_patchers = []


def _patcher(f):
    applied = False

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        nonlocal applied

        if applied:
            return

        applied = True
        return f(*args, **kwargs)

    _patchers.append(wrapper)
    return wrapper


def apply_all():
    for patcher in _patchers:
        patcher()


@_patcher
def io_no_tty():
    import sys

    for io in [sys.stdin, sys.stdout, sys.stderr]:
        if io:
            io.isatty = lambda: False


TQDM_MININTERVAL = 10


@_patcher
def tqdm_init():
    import tqdm

    original = tqdm.tqdm.__init__

    def patched(*args, **kwargs):
        mininterval = kwargs.get("mininterval")
        if mininterval is None or not isinstance(mininterval, (int, float)) or mininterval < TQDM_MININTERVAL:
            kwargs = kwargs.copy()
            kwargs["mininterval"] = TQDM_MININTERVAL

        return original(*args, **kwargs)

    tqdm.tqdm.__init__ = patched


@_patcher
def tqdm_display():
    import tqdm

    def tqdm_display(self, msg=None, pos=None):
        if pos is None:
            pos = abs(self.pos)

        if not msg:
            msg = str(self).replace("|| ", " - ")

        print(f"[tqdm:{pos}] {msg}")

    tqdm.tqdm.__init__ = functools.partialmethod(
        tqdm.tqdm.__init__,
        bar_format='{l_bar}{r_bar}'
    )

    tqdm.tqdm.display = tqdm_display


@_patcher
def tqdm_notebook():
    from tqdm.notebook import tqdm as tqdm_notebook

    for key, value in dict(vars(tqdm_notebook)).items():
        if callable(value) or isinstance(value, property):
            delattr(tqdm_notebook, key)


@_patcher
def pathlib_str_functions():
    import pathlib

    functions = [
        str.startswith,
        str.endswith
    ]

    def wrap(method):
        @functools.wraps(method)
        def wrapped(self: pathlib.PurePath, *args, **kwargs):
            return method(str(self), *args, **kwargs)
        return wrapped

    for function in functions:
        setattr(
            pathlib.PurePath,
            function.__name__,
            wrap(function)
        )


@_patcher
def keras_model_verbosity():
    try:
        import keras.src.engine.training
    except ModuleNotFoundError:
        return

    keras.src.engine.training._get_verbosity = lambda verbose, distribute_strategy: 0 if verbose == 0 else 2


@_patcher
def display_add():
    import builtins

    name = "display"
    if not hasattr(builtins, name):
        setattr(builtins, name, print)


CATBOOST_TRAIN_DIR_ENVVAR = "CATBOOST_TRAIN_DIR"
CATBOOST_TRAIN_DIR_KEY = "train_dir"


@_patcher
def catboost_info_directory():
    try:
        import catboost.core
    except ModuleNotFoundError:
        return

    _CatBoostBase = catboost.core._CatBoostBase
    original = _CatBoostBase.__init__

    def patched(self: _CatBoostBase, params):
        train_dir = os.getenv(CATBOOST_TRAIN_DIR_ENVVAR)

        if train_dir is not None:
            if params is None:
                params = {
                    CATBOOST_TRAIN_DIR_KEY: train_dir,
                }
            elif CATBOOST_TRAIN_DIR_KEY not in params:
                params[CATBOOST_TRAIN_DIR_KEY] = train_dir

        original(self, params)

    _CatBoostBase.__init__ = patched


@_patcher
def logging_file_handler():
    import logging

    original = logging.FileHandler.__init__

    def patched(self: logging.FileHandler, filename: str, *args, **kwargs):
        if not filename.startswith("/") and not os.access(filename, os.W_OK):
            filename = os.path.join("/tmp", filename)
            print(f"[debug] redirect logging file to: {filename}", file=sys.stderr)

        original(self, filename, *args, **kwargs)

    logging.FileHandler.__init__ = patched


@_patcher
def pycaret_internal_logging():
    try:
        import pycaret.internal.logging
    except ModuleNotFoundError:
        return

    pycaret.internal.logging.LOGGER = pycaret.internal.logging.create_logger("/dev/stdout")


@_patcher
def pickle_unpickler_find_class():
    """
    This is based on the assumption that the Python version of the `pickle` module is being used.
    The C version cannot be monkey patched. It's too bad; (it would have been nice)[https://github.com/python/cpython/blob/5ab66a882d1b5e44ec50b25df116ab209d65863f/Modules/_pickle.c#L5197].

    Both `pandas` and `joblib` use the Python version of the `pickle` module.
    """

    import pickle

    original = pickle._Unpickler.find_class

    def patched(self: pickle.Unpickler, module_name: str, global_name: str, /):
        from . import constants

        if constants.RUN_VIA_CLI and module_name == "__main__":
            module_name = constants.USER_CODE_MODULE_NAME

        return original(self, module_name, global_name)

    pickle._Unpickler.find_class = patched
