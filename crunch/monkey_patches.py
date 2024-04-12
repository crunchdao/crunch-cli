import functools
import os
import pathlib
import logging
import sys


_APPLIED = False


def apply_all():
    global _APPLIED

    if _APPLIED:
        return

    io_no_tty()
    tqdm_display()
    pathlib_str_functions()
    keras_model_verbosity()
    display_add()
    catboost_info_directory()
    logging_file_handler()
    pycaret_internal_logging()

    _APPLIED = True


def io_no_tty():
    import sys

    for io in [sys.stdin, sys.stdout, sys.stderr]:
        if io:
            io.isatty = lambda: False


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


def pathlib_str_functions():
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


def keras_model_verbosity():
    try:
        import keras.src.engine.training
    except ModuleNotFoundError:
        return

    keras.src.engine.training._get_verbosity = lambda verbose, distribute_strategy: 0 if verbose == 0 else 2


def display_add():
    import builtins

    name = "display"
    if not hasattr(builtins, name):
        setattr(builtins, name, print)


CATBOOST_TRAIN_DIR_ENVVAR = "CATBOOST_TRAIN_DIR"
CATBOOST_TRAIN_DIR_KEY = "train_dir"


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


def logging_file_handler():
    original = logging.FileHandler.__init__

    def patched(self: logging.FileHandler, filename: str, *args, **kwargs):
        if not filename.startswith("/") and not os.access(filename, os.W_OK):
            filename = os.path.join("/tmp", filename)
            print(f"[debug] redirect logging file to: {filename}", file=sys.stderr)

        original(self, filename, *args, **kwargs)

    logging.FileHandler.__init__ = patched


def pycaret_internal_logging():
    try:
        import pycaret.internal.logging
    except ModuleNotFoundError:
        return
    
    pycaret.internal.logging.LOGGER = pycaret.internal.logging.create_logger("/dev/stdout")
