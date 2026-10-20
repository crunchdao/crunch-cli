import functools
import importlib
import inspect
import os
import re
import sys
import traceback
from typing import Any, Callable

from . import constants

_patchers: list[Callable[[], None]] = []

SHOULD_PRINT_PREFIX_WHEN_POSSIBLE: str | None = None


def _patcher(function: Callable[[], None]) -> Callable[[], None]:
    applied = False

    @functools.wraps(function)
    def wrapper() -> None:
        nonlocal applied

        if applied:
            return

        applied = True
        function()

    _patchers.append(wrapper)
    return wrapper


def _try_import(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None


def apply_all():
    for patcher in _patchers:
        patcher()


@_patcher
def io_no_tty():
    import sys

    for io in [sys.stdin, sys.stdout, sys.stderr]:
        if io:
            setattr(io, "isatty", lambda: False)


TQDM_MININTERVAL = 10


@_patcher
def tqdm_init():
    import tqdm

    original: Callable[..., None] = getattr(tqdm.tqdm, "__init__")

    def patched(*args: Any, **kwargs: Any) -> None:
        mininterval = kwargs.get("mininterval")
        if mininterval is None or not isinstance(mininterval, (int, float)) or mininterval < TQDM_MININTERVAL:
            kwargs = kwargs.copy()
            kwargs["mininterval"] = TQDM_MININTERVAL

        return original(*args, **kwargs)

    setattr(tqdm.tqdm, "__init__", patched)


@_patcher
def tqdm_display():
    import tqdm

    def tqdm_display(self: Any, msg: str | None = None, pos: int | None = None) -> None:
        if pos is None:
            pos = abs(self.pos)

        if not msg:
            msg = str(self).replace("|| ", " - ")

        if SHOULD_PRINT_PREFIX_WHEN_POSSIBLE is not None:
            print(f"[{SHOULD_PRINT_PREFIX_WHEN_POSSIBLE}] [tqdm:{pos}] {msg}")
        else:
            print(f"[tqdm:{pos}] {msg}")

    setattr(tqdm.tqdm, "__init__", functools.partialmethod(
        getattr(tqdm.tqdm, "__init__"),
        bar_format='{l_bar}{r_bar}'
    ))

    setattr(tqdm.tqdm, "display", tqdm_display)


@_patcher
def tqdm_notebook():
    from tqdm.notebook import tqdm as tqdm_notebook

    for key, value in dict(vars(tqdm_notebook)).items():
        if callable(value) or isinstance(value, property):
            delattr(tqdm_notebook, key)


@_patcher
def pathlib_str_functions():
    import pathlib

    functions: list[Callable[..., bool]] = [
        str.startswith,
        str.endswith
    ]

    def wrap(method: Callable[..., bool]) -> Callable[..., bool]:
        @functools.wraps(method)
        def wrapped(self: pathlib.PurePath, *args: Any, **kwargs: Any) -> bool:
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
    training = _try_import("keras.src.engine.training")
    if training is None:
        return

    def get_verbosity(verbose: int, distribute_strategy: Any) -> int:
        return 0 if verbose == 0 else 2

    setattr(training, "_get_verbosity", get_verbosity)


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
    core = _try_import("catboost.core")
    if core is None:
        return

    catboost_base = getattr(core, "_CatBoostBase")
    original: Callable[..., None] = catboost_base.__init__

    def patched(self: Any, params: dict[str, Any] | None) -> None:
        train_dir = os.getenv(CATBOOST_TRAIN_DIR_ENVVAR)

        if train_dir is not None:
            if params is None:
                params = {
                    CATBOOST_TRAIN_DIR_KEY: train_dir,
                }
            elif CATBOOST_TRAIN_DIR_KEY not in params:
                params[CATBOOST_TRAIN_DIR_KEY] = train_dir

        original(self, params)

    setattr(catboost_base, "__init__", patched)


@_patcher
def logging_file_handler():
    import logging

    original: Callable[..., None] = logging.FileHandler.__init__

    def patched(self: logging.FileHandler, filename: str, *args: Any, **kwargs: Any) -> None:
        if not filename.startswith("/") and not os.access(filename, os.W_OK):
            filename = os.path.join("/tmp", filename)
            # print(f"[debug] redirect logging file to: {filename}", file=sys.stderr)

        original(self, filename, *args, **kwargs)

    setattr(logging.FileHandler, "__init__", patched)


@_patcher
def pycaret_internal_logging():
    internal_logging = _try_import("pycaret.internal.logging")
    if internal_logging is None:
        return

    internal_logging.LOGGER = internal_logging.create_logger("/dev/stdout")


def _should_redirect_main_to_user_code():
    """
    Determine if the __main__ module should be redirected to user_code:
    - If the code is run via the CLI
    - If the code is run in a loky child process
    """

    if constants.RUN_VIA_CLI:
        return True

    for frame, _ in traceback.walk_stack(inspect.currentframe()):
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

    python_unpickler = getattr(pickle, "_Unpickler")
    original: Callable[..., Any] = python_unpickler.find_class
    should_redirect: bool | None = None

    def patched(self: pickle.Unpickler, module_name: str, global_name: str) -> Any:
        nonlocal should_redirect
        if should_redirect is None:
            should_redirect = _should_redirect_main_to_user_code()

        if should_redirect and module_name == "__main__":
            module_name = constants.DEFAULT_USER_CODE_MODULE_NAME

        return original(self, module_name, global_name)

    setattr(python_unpickler, "find_class", patched)
    setattr(multiprocessing.reduction.ForkingPickler, "loads", getattr(pickle, "_loads"))


@_patcher
def joblib_parallel_initializer():
    joblib = _try_import("joblib")
    loky_initializers = _try_import("joblib.externals.loky.initializers")
    if joblib is None or loky_initializers is None:
        return

    original: Callable[..., None] = joblib.Parallel.__init__

    def patched(self: Any, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)

        prepare_for_process_initializer()

        # Pretty unstable as it relies on private variables
        backend_arguments: dict[str, Any] = getattr(self, "_backend_args", None) or getattr(self, "_backend_kwargs", None) or {}

        initializer = backend_arguments.pop("initializer", None)
        if initializer is None:
            backend_arguments["initializer"] = process_initializer
        else:
            chain_initializers: Callable[..., tuple[Any, Any]] = getattr(loky_initializers, "_chain_initializers")
            initializer, initializer_arguments = chain_initializers([
                (process_initializer, ()),
                (initializer, backend_arguments.pop("initargs", None) or ())
            ])

            backend_arguments["initargs"] = initializer_arguments
            backend_arguments["initializer"] = initializer

    setattr(joblib.Parallel, "__init__", patched)


def prepare_for_process_initializer(
    module_name: str = constants.DEFAULT_USER_CODE_MODULE_NAME
) -> None:
    user_code_module = sys.modules.get(module_name)

    user_code_path = user_code_module.__file__ if user_code_module is not None else None
    user_code_path = user_code_path or os.environ.get(constants.MAIN_FILE_PATH_ENV_VAR)
    user_code_path = user_code_path or os.path.join(os.getcwd(), constants.DEFAULT_MAIN_FILE_PATH)

    os.environ[constants.USER_CODE_MODULE_NAME_ENV_VAR] = module_name
    os.environ[constants.MAIN_FILE_PATH_ENV_VAR] = user_code_path


def process_initializer(*args: Any, **kwargs: Any) -> None:
    apply_all()

    user_code_module_name = os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR, constants.DEFAULT_USER_CODE_MODULE_NAME)
    main_file_path = os.environ.get(constants.MAIN_FILE_PATH_ENV_VAR, constants.DEFAULT_MAIN_FILE_PATH)

    if not user_code_module_name in sys.modules:
        from .command.test import load_user_code
        module = load_user_code(main_file_path, user_code_module_name)

        sys.modules[user_code_module_name] = module
