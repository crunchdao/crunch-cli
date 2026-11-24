import functools
import importlib
import inspect
import os
import re
import sys
import traceback
from typing import Any, Callable, ClassVar

from . import constants

SHOULD_PRINT_PREFIX_WHEN_POSSIBLE: str | None = None


class Patch:

    _registered: ClassVar[list["Patch"]] = []

    def __init__(self, function: Callable[[], None], module_name: str | None):
        functools.update_wrapper(self, function)

        self._function = function
        self.module_name = module_name
        self._applied = False

    def __call__(self) -> None:
        if self._applied:
            return

        if self.module_name is None or self.module_name in sys.modules:
            self.apply()
        else:
            _get_post_import_finder().watch(self.module_name, self)

    def apply(self) -> None:
        if self._applied:
            return

        self._applied = True
        self._function()

    @classmethod
    def register(cls, module_name: str | None = None) -> Callable[[Callable[[], None]], "Patch"]:
        """
        Without `module_name`, the patch is applied by `apply_all()`.
        With it, the patch is applied right after the first import of the module, or right away if it is already imported, so that `apply_all()` does not import it.
        """

        def decorator(function: Callable[[], None]) -> "Patch":
            patch = cls(function, module_name)
            cls._registered.append(patch)

            return patch

        return decorator

    @classmethod
    def apply_all(cls) -> None:
        for patch in cls._registered:
            patch()


class _PostImportFinder:
    """
    Only handles the watched modules: it finds them with the other finders, and wraps their loader to apply the patches after the module runs.
    """

    def __init__(self):
        self._patches_by_module_name: dict[str, list[Patch]] = {}

    def watch(self, module_name: str, patch: Patch) -> None:
        self._patches_by_module_name.setdefault(module_name, []).append(patch)

    def take(self, module_name: str) -> list[Patch]:
        return self._patches_by_module_name.pop(module_name, [])

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> Any:
        """
        The patches stay watched until the module runs: `importlib.util.find_spec` also comes here, to only check that a module is installed.
        """

        if fullname not in self._patches_by_module_name:
            return None

        spec = self._find_spec_with_other_finders(fullname, path, target)
        if spec is None or spec.loader is None or not hasattr(spec.loader, "exec_module"):
            return spec

        spec.loader = _PatchingLoader(spec.loader, fullname, self)
        return spec

    def _find_spec_with_other_finders(self, fullname: str, path: Any, target: Any) -> Any:
        for finder in sys.meta_path:
            if finder is self:
                continue

            find_spec = getattr(finder, "find_spec", None)
            if find_spec is None:
                continue

            spec = find_spec(fullname, path, target)
            if spec is not None:
                return spec

        return None


class _PatchingLoader:
    """
    Puts the real loader back on the module before running it, so that the module looks imported normally, then applies the patches.
    """

    def __init__(self, loader: Any, module_name: str, finder: _PostImportFinder):
        self._loader = loader
        self._module_name = module_name
        self._finder = finder

    def __getattr__(self, name: str) -> Any:
        return getattr(self._loader, name)

    def exec_module(self, module: Any) -> None:
        module.__loader__ = self._loader
        if module.__spec__ is not None:
            module.__spec__.loader = self._loader

        self._loader.exec_module(module)

        for patch in self._finder.take(self._module_name):
            try:
                patch.apply()
            except Exception as error:
                print(f"crunch: could not apply the `{patch.__name__}` patch: {error.__class__.__name__}: {error}", file=sys.stderr)


_post_import_finder: _PostImportFinder | None = None


def _get_post_import_finder() -> _PostImportFinder:
    global _post_import_finder

    if _post_import_finder is None:
        _post_import_finder = _PostImportFinder()
        sys.meta_path.insert(0, _post_import_finder)

    return _post_import_finder


def _try_import(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None


apply_all = Patch.apply_all


@Patch.register()
def io_no_tty():
    import sys

    for io in [sys.stdin, sys.stdout, sys.stderr]:
        if io:
            setattr(io, "isatty", lambda: False)


TQDM_MININTERVAL = 10


@Patch.register("tqdm")
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


@Patch.register("tqdm")
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


@Patch.register("tqdm.notebook")
def tqdm_notebook():
    from tqdm.notebook import tqdm as tqdm_notebook

    for key, value in dict(vars(tqdm_notebook)).items():
        if callable(value) or isinstance(value, property):
            delattr(tqdm_notebook, key)


@Patch.register()
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


@Patch.register("keras.src.engine.training")
def keras_model_verbosity():
    training = importlib.import_module("keras.src.engine.training")

    def get_verbosity(verbose: int, distribute_strategy: Any) -> int:
        return 0 if verbose == 0 else 2

    setattr(training, "_get_verbosity", get_verbosity)


@Patch.register()
def display_add():
    import builtins

    name = "display"
    if not hasattr(builtins, name):
        setattr(builtins, name, print)


CATBOOST_TRAIN_DIR_ENVVAR = "CATBOOST_TRAIN_DIR"
CATBOOST_TRAIN_DIR_KEY = "train_dir"


@Patch.register("catboost.core")
def catboost_info_directory():
    core = importlib.import_module("catboost.core")

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


@Patch.register()
def logging_file_handler():
    import logging

    original: Callable[..., None] = logging.FileHandler.__init__

    def patched(self: logging.FileHandler, filename: str, *args: Any, **kwargs: Any) -> None:
        if not filename.startswith("/") and not os.access(filename, os.W_OK):
            filename = os.path.join("/tmp", filename)
            # print(f"[debug] redirect logging file to: {filename}", file=sys.stderr)

        original(self, filename, *args, **kwargs)

    setattr(logging.FileHandler, "__init__", patched)


@Patch.register("pycaret.internal.logging")
def pycaret_internal_logging():
    internal_logging = importlib.import_module("pycaret.internal.logging")

    internal_logging.LOGGER = internal_logging.create_logger("/dev/stdout")


def _should_redirect_main_to_user_code():
    """
    Determine if the __main__ module should be redirected to user_code:
    - If the code is run via the CLI
    - If the process, or one of its parents, loaded the user's code
    - If the code is run in a loky child process
    """

    if constants.RUN_VIA_CLI or os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR):
        return True

    for frame, _ in traceback.walk_stack(inspect.currentframe()):
        file_name = frame.f_code.co_filename
        if re.search(r"loky(?:\\|/)backend(?:\\|/)popen_", file_name):
            return True

    return False


def _redirect_main_to_user_code(module_name: str) -> str:
    """
    Redirects only when the user's code can be found: in a notebook, the user's code is the real `__main__`.
    """

    if module_name != "__main__" or not _should_redirect_main_to_user_code():
        return module_name

    import importlib.util

    user_code_module_name = os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR) or constants.DEFAULT_USER_CODE_MODULE_NAME
    if user_code_module_name in sys.modules or importlib.util.find_spec(user_code_module_name) is not None:
        return user_code_module_name

    return module_name


@Patch.register()
def pickle_unpickler_find_class():
    """
    Code trained outside of crunch, like in a notebook or with `python main.py`, is pickled as `__main__.<name>`: it is looked up in the user's code instead.

    `pickle.Unpickler`, `pickle.load` and `pickle.loads` are replaced by a subclass of the C unpickler, which stays as fast since only `find_class` runs in Python.
    The Python unpickler, that `pandas` and `joblib` use, is patched as well.
    """

    import io
    import multiprocessing.reduction
    import pickle

    class Unpickler(pickle.Unpickler):

        def find_class(self, module_name: str, global_name: str) -> Any:
            return super().find_class(_redirect_main_to_user_code(module_name), global_name)

    def load(file: Any, *, fix_imports: bool = True, encoding: str = "ASCII", errors: str = "strict", buffers: Any = None) -> Any:
        return Unpickler(file, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers).load()

    def loads(data: Any, /, *, fix_imports: bool = True, encoding: str = "ASCII", errors: str = "strict", buffers: Any = None) -> Any:
        if isinstance(data, str):
            raise TypeError("Can't load pickle from unicode string")

        return Unpickler(io.BytesIO(data), fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers).load()

    python_unpickler = getattr(pickle, "_Unpickler")
    original_python_find_class: Callable[..., Any] = python_unpickler.find_class

    def python_find_class(self: Any, module_name: str, global_name: str) -> Any:
        return original_python_find_class(self, _redirect_main_to_user_code(module_name), global_name)

    setattr(python_unpickler, "find_class", python_find_class)

    setattr(pickle, "Unpickler", Unpickler)
    setattr(pickle, "load", load)
    setattr(pickle, "loads", loads)
    setattr(multiprocessing.reduction.ForkingPickler, "loads", staticmethod(loads))


@Patch.register("joblib")
def joblib_parallel_initializer():
    joblib = importlib.import_module("joblib")

    loky_initializers = _try_import("joblib.externals.loky.initializers")
    if loky_initializers is None:
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
    user_code_path = user_code_path or os.environ.get(constants.USER_CODE_MAIN_FILE_PATH_ENV_VAR)
    user_code_path = user_code_path or os.path.join(os.getcwd(), constants.DEFAULT_MAIN_FILE_PATH)

    os.environ[constants.USER_CODE_MODULE_NAME_ENV_VAR] = module_name
    os.environ[constants.USER_CODE_MAIN_FILE_PATH_ENV_VAR] = user_code_path


def process_initializer(*args: Any, **kwargs: Any) -> None:
    apply_all()

    user_code_module_name = os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR, constants.DEFAULT_USER_CODE_MODULE_NAME)
    main_file_path = os.environ.get(constants.USER_CODE_MAIN_FILE_PATH_ENV_VAR, constants.DEFAULT_MAIN_FILE_PATH)

    if user_code_module_name not in sys.modules:
        from .command.test import load_user_code
        load_user_code(main_file_path, user_code_module_name)


@Patch.register()
def user_code_finder():
    """
    A process started with `spawn` or `forkserver` does not inherit the user's code, and imports it again to unpickle one of its functions.
    Registers a finder that loads it from the main file that the parent process exported with `prepare_for_process_initializer`.
    """

    module_name = os.environ.get(constants.USER_CODE_MODULE_NAME_ENV_VAR)
    main_file_path = os.environ.get(constants.USER_CODE_MAIN_FILE_PATH_ENV_VAR)
    if not module_name or not main_file_path:
        return

    sys.meta_path.append(_UserCodeFinder(module_name, os.path.abspath(main_file_path)))


class _UserCodeFinder:

    def __init__(self, module_name: str, main_file_path: str):
        self._module_name = module_name
        self._main_file_path = main_file_path

    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        if fullname != self._module_name or not os.path.isfile(self._main_file_path):
            return None

        import importlib.util

        return importlib.util.spec_from_file_location(fullname, self._main_file_path)


def apply_for_user_code_child_process():
    """
    Applied at startup by `crunch-monkeypatch.pth` in a child process of the user's code.
    """

    user_code_finder()
    pickle_unpickler_find_class()
