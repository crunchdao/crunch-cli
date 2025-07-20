import functools
import os
import re
import sys
import traceback

from . import constants

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
            # print(f"[debug] redirect logging file to: {filename}", file=sys.stderr)

        original(self, filename, *args, **kwargs)

    logging.FileHandler.__init__ = patched


@_patcher
def pycaret_internal_logging():
    try:
        import pycaret.internal.logging
    except ModuleNotFoundError:
        return

    pycaret.internal.logging.LOGGER = pycaret.internal.logging.create_logger("/dev/stdout")


def _should_redirect_main_to_user_code():
    """
    Determine if the __main__ module should be redirected to user_code:
    - If the code is run via the CLI
    - If the code is run in a loky child process
    """

    if constants.RUN_VIA_CLI:
        return True

    for frame, _ in traceback.walk_stack(sys._getframe()):
        file_name = frame.f_code.co_filename
        if re.search(r"loky(?:\\|/)backend(?:\\|/)popen_", file_name):
            return True

    return False


@_patcher
def pickle_unpickler_find_class():
    """
    This is based on the assumption that the Python version of the `pickle` module is being used.
    The C version cannot be monkey patched. It's too bad; (it would have been nice)[https://github.com/python/cpython/blob/5ab66a882d1b5e44ec50b25df116ab209d65863f/Modules/_pickle.c#L5197].

    Both `pandas` and `joblib` use the Python version of the `pickle` module.
    """

    import multiprocessing.reduction
    import pickle

    original = pickle._Unpickler.find_class
    should_redirect = None

    def patched(self: pickle.Unpickler, module_name: str, global_name: str):
        nonlocal should_redirect
        if should_redirect is None:
            should_redirect = _should_redirect_main_to_user_code()

        if should_redirect and module_name == "__main__":
            module_name = constants.DEFAULT_USER_CODE_MODULE_NAME

        return original(self, module_name, global_name)

    pickle._Unpickler.find_class = patched
    multiprocessing.reduction.ForkingPickler.loads = pickle._loads


@_patcher
def joblib_parallel_initializer():
    try:
        import joblib
        import joblib.externals.loky.initializers
    except ModuleNotFoundError:
        return

    original = joblib.Parallel.__init__

    def patched(self, *args, **kwargs):
        original(self, *args, **kwargs)

        prepare_for_process_initializer()

        # Pretty unstable as it relies on private variables
        args = getattr(self, "_backend_args", None) or getattr(self, "_backend_kwargs", None) or {}

        initializer = args.pop("initializer", None)
        if initializer is None:
            args["initializer"] = process_initializer
        else:
            initializer, initargs = joblib.externals.loky.initializers._chain_initializers([
                (process_initializer, ()),
                (initializer, args.pop("initargs", None) or ())
            ])

            args["initargs"] = initargs
            args["initializer"] = initializer

    joblib.Parallel.__init__ = patched


def prepare_for_process_initializer(
    module_name=constants.DEFAULT_USER_CODE_MODULE_NAME
):
    user_code_module = sys.modules.get(module_name)

    user_code_path = user_code_module.__file__ if user_code_module is not None else None
    user_code_path = user_code_path or os.environ.get(constants.MAIN_FILE_PATH_ENV_VAR)
    user_code_path = user_code_path or os.path.join(os.getcwd(), constants.DEFAULT_MAIN_FILE_PATH)

    os.environ[constants.USER_CODE_MODULE_NAME_ENV_VAR] = module_name
    os.environ[constants.MAIN_FILE_PATH_ENV_VAR] = user_code_path


def process_initializer(*args, **kwargs):
    apply_all()

    user_code_module_name = os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR, constants.DEFAULT_USER_CODE_MODULE_NAME)
    main_file_path = os.environ.get(constants.MAIN_FILE_PATH_ENV_VAR, constants.DEFAULT_MAIN_FILE_PATH)

    if not user_code_module_name in sys.modules:
        from .command.test import load_user_code
        module = load_user_code(main_file_path, user_code_module_name)

        sys.modules[user_code_module_name] = module
