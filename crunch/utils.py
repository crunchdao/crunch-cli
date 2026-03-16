import datetime
import json
import logging
import os
import shutil
import time
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Dict, Generic, Iterable, Literal, NoReturn, Optional, Set, Type, TypeVar, Union, cast, overload

import click
import requests
from tqdm.auto import tqdm

from crunch.constants import DOT_CRUNCH_DIRECTORY, PROJECT_FILE, TOKEN_FILE

if TYPE_CHECKING:
    from crunch.api import ApiException, SizeVariant


def change_root():
    while True:
        current = os.getcwd()

        if os.path.exists(DOT_CRUNCH_DIRECTORY):
            print(f"project: found {current}")
            return

        os.chdir("../")
        if current == os.getcwd():
            print("project: not found")
            raise click.Abort()


if TYPE_CHECKING:
    @overload
    def _read_crunch_file(
        name: str,
        raise_if_missing: Literal[True] = True,
    ) -> str:
        ...

    @overload
    def _read_crunch_file(
        name: str,
        raise_if_missing: Literal[False] = False,
    ) -> Optional[str]:
        ...


def _read_crunch_file(
    name: str,
    raise_if_missing: bool = True,
):
    path = os.path.join(DOT_CRUNCH_DIRECTORY, name)

    if not os.path.exists(path):
        if raise_if_missing:
            print(f"{path}: not found, are you in the project directory?")
            print(f"{path}: make sure to `cd <competition>` first")
            raise click.Abort()

        return None

    with open(path) as fd:
        return fd.read()


def write_token(plain_push_token: str, directory: str = "."):
    dot_crunch_path = os.path.join(
        directory,
        DOT_CRUNCH_DIRECTORY
    )

    token_file_path = os.path.join(dot_crunch_path, TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(plain_push_token)


@dataclass
class ProjectInfo:
    competition_name: str
    project_name: str
    user_id: int
    size_variant: "SizeVariant"


def write_project_info(info: ProjectInfo, directory: str = "."):
    dot_crunch_path = os.path.join(
        directory,
        DOT_CRUNCH_DIRECTORY
    )

    path = os.path.join(dot_crunch_path, PROJECT_FILE)
    with open(path, "w") as fd:
        json.dump({
            "competitionName": info.competition_name,
            "projectName": info.project_name,
            "userId": info.user_id,
            "sizeVariant": info.size_variant.name,
        }, fd)


if TYPE_CHECKING:
    @overload
    def read_project_info(
        raise_if_missing: Literal[True] = True,
    ) -> ProjectInfo:
        ...

    @overload
    def read_project_info(
        raise_if_missing: Literal[False] = False,
    ) -> Optional[ProjectInfo]:
        ...


def read_project_info(
    raise_if_missing: bool = True,
) -> Optional[ProjectInfo]:
    from crunch.api import SizeVariant

    content = _read_crunch_file(PROJECT_FILE, raise_if_missing)
    if not raise_if_missing and content is None:
        return None

    assert content is not None
    root = json.loads(content)

    try:
        size_variant = SizeVariant[root["sizeVariant"]]
    except:
        size_variant = SizeVariant.DEFAULT

    # TODO: need of a better system for handling file versions
    return ProjectInfo(
        root["competitionName"],
        root.get("projectName") or "default",  # backward compatibility
        root["userId"],
        size_variant,
    )


def try_get_competition_name():
    project_info = read_project_info(False)

    if project_info is None:
        return None

    return project_info.competition_name


def read_token():
    return _read_crunch_file(TOKEN_FILE)


def get_process_memory() -> int:
    import psutil
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


class _undefined:
    pass


_smart_call_ignore: Set[str] = set()
_T = TypeVar("_T")


class LazyValue(Generic[_T]):

    def __init__(self, factory: Callable[[], _T]):
        self._factory = factory
        self._value: Union[Type[_undefined], _T] = _undefined

    @property
    def value(self) -> _T:
        if self._value is _undefined:
            self._value = self._factory()

        return cast(_T, self._value)


def smart_call(
    function: Callable[..., _T],
    default_values: Dict[str, Any],
    specific_values: Dict[str, Any] = {},
    log: bool = True,
    logger: logging.Logger = logging.getLogger(),
) -> _T:
    import inspect

    values = {
        **default_values,
        **specific_values
    }

    def warn(message: str):
        if log and message not in _smart_call_ignore:
            _smart_call_ignore.add(message)
            logger.warning(f"{function.__name__}: {message}")

    def debug(message: str):
        if log and message not in _smart_call_ignore:
            _smart_call_ignore.add(message)
            logger.debug(f"{function.__name__}: {message}")

    arguments = {}
    for name, parameter in inspect.signature(function).parameters.items():
        name_str = str(parameter)
        if name_str.startswith("*"):
            warn(f"unsupported parameter: {name_str}")
            continue

        if parameter.default != inspect.Parameter.empty:
            warn(f"skip param with default value: {name}={parameter.default}")
            continue

        value = values.get(name, _undefined)
        if value is _undefined:
            warn(f"unknown parameter: {name}")
            value = None

        if isinstance(value, LazyValue):
            value = value.value  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]

        debug(f"set {name}={value.__class__.__name__}")  # pyright: ignore[reportUnknownMemberType]
        arguments[name] = value

    return function(**arguments)


def cut_url(url: str):
    try:
        url = url[:url.index("?")]
    except ValueError:
        pass

    if url.startswith("http://") or url.startswith("https://"):
        return url.replace("//", "", 1)

    return url


def _download_head(
    session: requests.Session,
    url: str,
    path: str,
    log: bool,
    print: Callable[[str], None],
):
    logged = False
    response: Optional[requests.Response] = None

    try:
        response = session.get(url, stream=True)
        response.raise_for_status()

        file_length = response.headers.get("Content-Length", None)
        file_length = int(file_length) if file_length is not None else None

        if log:
            if file_length is not None:
                file_length_str = f"{file_length} bytes"
            else:
                file_length_str = "unknown length"

            print(f"{path}: download from {cut_url(url)} ({file_length_str})")
            logged = True

        accept_ranges = response.headers.get("Accept-Ranges", None)
        accept_ranges = "bytes" == accept_ranges

        return file_length, accept_ranges, response
    except:
        if log and not logged:
            print(f"downloading {path} from {cut_url(url)}")

        if response is not None:  # type: ignore
            response.close()

        raise


def download(
    url: str,
    path: str,
    log: bool = True,
    print: Callable[[str], None] = print,
    progress_bar: bool = True,
    max_retry: int = 10,
    session: Optional[requests.Session] = None,
):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    if session is None:
        session = requests.Session()

    session.headers["Accept-Encoding"] = "identity"  # GitHub provide the range on the gzip-encoded response instead of re-encoding it

    file_length, accept_ranges, response = _download_head(session, url, path, log, print)

    if not accept_ranges:
        max_retry = 0

    total_read = 0

    file_name = os.path.basename(path)

    with TemporaryDirectory(
        prefix=f"{file_name}.",
        dir=os.path.dirname(path),
    ) as temporary_directory_path:
        source_file_path = os.path.join(temporary_directory_path, file_name)
        destination_file_path = path

        with open(source_file_path, 'wb') as fd:
            for retry in range(max_retry + 1):
                last = retry == max_retry

                headers = {
                    "Range": f"bytes={total_read}-",
                } if retry else {}

                try:
                    response = response or session.get(
                        url,
                        stream=True,
                        headers=headers,
                        timeout=30,
                    )

                    response.raise_for_status()

                    with tqdm(
                        initial=total_read,
                        total=file_length,
                        unit='iB',
                        unit_scale=True,
                        leave=False,
                        disable=not progress_bar
                    ) as progress:
                        for chunk in response.iter_content(chunk_size=1024 * 16):
                            chunk_size = len(chunk)
                            total_read += chunk_size

                            progress.update(chunk_size)
                            fd.write(chunk)

                    break
                except (requests.exceptions.ConnectionError, KeyboardInterrupt) as error:
                    if last:
                        raise

                    print(f"retrying {retry + 1}/{max_retry} at {total_read} bytes because of {error.__class__.__name__}: {str(error) or '(no message)'}")
                    time.sleep(1)
                finally:
                    if response is not None:
                        response.close()

                    response = None

        if os.path.exists(destination_file_path):
            os.unlink(destination_file_path)

        shutil.move(
            source_file_path,
            destination_file_path
        )


def exit_via(error: "ApiException", **kwargs: Any) -> NoReturn:
    print("\n---")
    error.print_helper(**kwargs)
    exit(1)


class Tracer:

    def __init__(self, printer: Callable[[str], None] = print):
        self._depth = 0
        self._printer = printer

    @property
    def padding(self):
        return "  " * self._depth

    def loop(
        self,
        iterable: Iterable[_T],
        action: Union[str, Callable[[_T], str]],
        value_placeholder: str = "{value}",
    ):
        has_value_placeholder = False
        if not callable(action):
            action = str(action)
            has_value_placeholder = value_placeholder in action

        for value in iterable:
            if callable(action):
                action_message = str(action(value))
            else:
                action_message = action

                if has_value_placeholder:
                    action_message = action_message.replace(value_placeholder, str(value))

            with self.log(action_message):
                yield value

    @contextmanager
    def log(
        self,
        action: str,
    ):
        start = datetime.datetime.now()
        self._printer(f"{start} {self.padding} {action}")

        try:
            self._depth += 1

            yield True
        finally:
            self._depth -= 1

            import gc
            gc.collect()

            end = datetime.datetime.now()
            self._printer(f"{start} {self.padding} {action} took {end - start}")


class LimitedSizeIO:

    def __init__(
        self,
        underlying_io: BinaryIO,
        limit: int,
        callback: Optional[Callable[[int], None]] = None
    ):
        self.underlying_io = underlying_io
        self.limit = limit
        self.callback = callback
        self.read_so_far = 0

    def read(self, size: int = -1):
        if self.read_so_far >= self.limit:
            return b''

        if size == -1:
            size = self.limit - self.read_so_far
        else:
            size = min(size, self.limit - self.read_so_far)

        data = self.underlying_io.read(size)
        data_length = len(data)

        self.read_so_far += data_length

        if self.callback is not None:
            self.callback(data_length)

        return data

    def readable(self):
        return True

    def __len__(self):
        return self.limit
