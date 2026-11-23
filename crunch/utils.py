import logging
import os
import re
import sys
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Generic, Iterable, Sequence, TypeVar, cast

if TYPE_CHECKING:
    import requests


def get_process_memory() -> int:
    import psutil
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


class _undefined:
    pass


_smart_call_ignore: set[str] = set()
_T = TypeVar("_T")


class LazyValue(Generic[_T]):

    def __init__(self, factory: Callable[[], _T]):
        self._factory = factory
        self._value: type[_undefined] | _T = _undefined

    @property
    def value(self) -> _T:
        if self._value is _undefined:
            self._value = self._factory()

        return cast(_T, self._value)


def smart_call(
    function: Callable[..., _T],
    default_values: dict[str, Any],
    specific_values: dict[str, Any] = {},
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


def split_into_chunks(items: list[_T], chunk_size: int) -> Iterable[list[_T]]:
    for start_index in range(0, len(items), chunk_size):
        yield items[start_index:start_index + chunk_size]


if sys.version_info >= (3, 11):
    def parse_datetime(value: str) -> datetime:
        return datetime.fromisoformat(value)

else:
    _DATETIME_FRACTION = re.compile(r"\.(\d+)")

    def parse_datetime(value: str) -> datetime:
        """
        `datetime.fromisoformat` of python 3.10 only accepts 3 or 6 fraction digits and no `Z`,
        but the API trims the trailing zeros of the fraction (`.3459`).
        """

        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        value = _DATETIME_FRACTION.sub(
            lambda match: "." + match.group(1).ljust(6, "0")[:6],
            value,
            count=1,
        )

        return datetime.fromisoformat(value)


def cut_url(url: str):
    """Deprecated, use `crunch.download.cut_url` instead."""

    from crunch.download import cut_url

    _warn_deprecated("cut_url", "cut_url")

    return cut_url(url)


def download(
    url: str,
    path: str,
    log: bool = True,
    print: Callable[[str], Any] = print,
    progress_bar: bool = True,
    max_retry: int = 10,
    session: "requests.Session | None" = None,
):
    """Deprecated, use `crunch.download.download_file` instead."""

    from crunch.download import download_file
    from crunch.downloader import _to_logger

    _warn_deprecated("download", "download_file")

    download_file(
        url,
        path,
        logger=_to_logger(print),
        verbose=log,
        progress_bar=progress_bar,
        max_retry=max_retry,
        session=session,
    )


def _warn_deprecated(name: str, replacement: str):
    warnings.warn(
        f"`crunch.utils.{name}` is deprecated and will be removed in the next major version, use `crunch.download.{replacement}` instead.",
        DeprecationWarning,
        stacklevel=3,
    )


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
        action: str | Callable[[_T], str],
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
        start = datetime.now()
        self._printer(f"{start} {self.padding} {action}")

        try:
            self._depth += 1

            yield True
        finally:
            self._depth -= 1

            import gc
            gc.collect()

            end = datetime.now()
            self._printer(f"{start} {self.padding} {action} took {end - start}")


class LimitedSizeIO:

    def __init__(
        self,
        underlying_io: BinaryIO,
        limit: int,
        callback: Callable[[int], None] | None = None
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


def is_running_inside_notebook():
    try:
        get_ipython()  # pyright: ignore[reportUndefinedVariable]
        return True
    except NameError:
        return False


def to_unix_path(input: str):
    if input == ".":
        return input + "/"

    has_trailing_slash = input.endswith(("/", "\\"))

    input = os.path.normpath(input)\
        .replace("\\", "/")\
        .replace("//", "/")

    if has_trailing_slash and not input.endswith("/"):
        input += "/"

    return input


def build_user_agent() -> str:
    import platform
    import sys

    from crunch.__version__ import __version__ as crunch_version

    python_version = ".".join(map(str, sys.version_info[:3]))
    os_name = platform.system()  # "Linux", "Darwin", "Windows"

    return f"crunch-cli/{crunch_version} (Python/{python_version}; {os_name})"


def format_duration(duration: timedelta) -> str:
    """Compact, like `1:08:15`: what `str()` gives, without the microseconds."""

    return str(timedelta(seconds=int(duration.total_seconds())))


def shorten(text: str, length: int) -> str:
    """Cut to `length` characters, ending with `...` when cut. Unlike `textwrap.shorten`, it does not need spaces to cut."""

    return text if len(text) <= length else text[:length - 3] + "..."


def ascii_table(
    *,
    headers: Sequence[str],
    values: list[Sequence[Any]],
    spacing: int = 3,
):
    rows = [
        list(map(str, row))
        for row in values
    ]

    header_liness = [
        header.split("\n")
        for header in headers
    ]

    max_header_lines_count = max(len(header_lines) for header_lines in header_liness)
    for _ in range(max_header_lines_count):
        rows.insert(0, [""] * len(headers))

    for index, header_lines in enumerate(header_liness):
        for line_index, line in enumerate(header_lines):
            # Headers are lists, so they are indexable and mutable.
            rows[line_index][index] = line  # pyright: ignore[reportIndexIssue]

    max_length_per_columns = [
        max((len(row[index]) for row in rows))
        for index in range(len(rows[0]))
    ]

    separators = [
        "-" * (max_length_per_columns[index])
        for index in range(len(max_length_per_columns))
    ]
    rows.insert(max_header_lines_count, separators)

    for index, row in enumerate(rows):
        print("  ", end="")

        for column_index, value in enumerate(row):
            width = max_length_per_columns[column_index] + spacing
            print(value.ljust(width), end="")

        print()
