import contextlib
import dataclasses
import datetime
import functools
import gc
import inspect
import io
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import typing

import click
import joblib
import pandas
import requests
import tqdm

from . import api, constants


def change_root():
    while True:
        current = os.getcwd()

        if os.path.exists(constants.DOT_CRUNCHDAO_DIRECTORY):
            print(f"project: found {current}")
            return

        os.chdir("../")
        if current == os.getcwd():
            print("project: not found")
            raise click.Abort()


def _read_crunchdao_file(name: str, raise_if_missing=True):
    path = os.path.join(constants.DOT_CRUNCHDAO_DIRECTORY, name)

    if not os.path.exists(path):
        if raise_if_missing:
            print(f"{path}: not found, are you in the project directory?")
            print(f"{path}: make sure to `cd <competition>` first")
            raise click.Abort()

        return None

    with open(path) as fd:
        return fd.read()


def write_token(plain_push_token: str, directory="."):
    dot_crunchdao_path = os.path.join(
        directory,
        constants.DOT_CRUNCHDAO_DIRECTORY
    )

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(plain_push_token)


@dataclasses.dataclass()
class ProjectInfo:
    competition_name: str
    project_name: str
    user_id: str
    size_variant: api.SizeVariant


def write_project_info(info: ProjectInfo, directory=".") -> ProjectInfo:
    dot_crunchdao_path = os.path.join(
        directory,
        constants.DOT_CRUNCHDAO_DIRECTORY
    )

    old_path = os.path.join(dot_crunchdao_path, constants.OLD_PROJECT_FILE)
    if os.path.exists(old_path):
        os.remove(old_path)

    path = os.path.join(dot_crunchdao_path, constants.PROJECT_FILE)
    with open(path, "w") as fd:
        json.dump({
            "competitionName": info.competition_name,
            "projectName": info.project_name,
            "userId": info.user_id,
            "sizeVariant": info.size_variant.name,
        }, fd)


def read_project_info(raise_if_missing=True) -> ProjectInfo:
    old_content = _read_crunchdao_file(constants.OLD_PROJECT_FILE, False)
    if old_content is not None:
        return ProjectInfo(
            "adialab",
            "default",
            root["userId"],
        )

    content = _read_crunchdao_file(constants.PROJECT_FILE, raise_if_missing)
    if not raise_if_missing and content is None:
        return None

    root = json.loads(content)

    try:
        size_variant = api.SizeVariant[root["sizeVariant"]]
    except:
        size_variant = api.SizeVariant.DEFAULT

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
    return _read_crunchdao_file(constants.TOKEN_FILE)


def read(path: str, kwargs={}) -> typing.Any:
    if path.endswith(".parquet"):
        return pandas.read_parquet(path, **kwargs)

    if path.endswith(".csv"):
        return pandas.read_csv(path, **kwargs)

    if path.endswith(".pickle"):
        return pandas.read_pickle(path, **kwargs)

    return joblib.load(path)


def write(dataframe: typing.Any, path: str, kwargs={}) -> None:
    if path.endswith(".parquet"):
        return dataframe.to_parquet(path, **kwargs)

    if path.endswith(".csv"):
        return dataframe.to_csv(path, **kwargs)

    if path.endswith(".pickle"):
        return pandas.to_pickle(dataframe, path, **kwargs)

    return joblib.dump(dataframe, path)


def strip_python_special_lines(lines: typing.List[str]):
    return "\n".join(
        line
        for line in lines
        if not re.match(r"^\s*?(!|%|#)", line)
    )


def pip_freeze():
    import importlib_metadata

    working_set = {}

    installed_packages = {
        package
        for packages in importlib_metadata.packages_distributions().values()
        for package in packages
    }

    for package in installed_packages:
        version = importlib_metadata.version(package)

        if not is_valid_version(version):
            continue

        working_set[package] = version

    return working_set


def is_valid_version(input: str):
    import packaging.version

    try:
        packaging.version.Version(input)
        return True
    except:
        return False


def get_process_memory() -> int:
    import psutil
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


def format_bytes(bytes: int):
    from .external import humanfriendly

    return humanfriendly.format_size(bytes)


class _undefined:
    pass


_smart_call_ignore = set()


def smart_call(
    function: callable,
    default_values: dict,
    specific_values={},
    log=True,
    logger=logging.getLogger(),
    limit_traceback=False,
):
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

        debug(f"set {name}={value.__class__.__name__}")
        arguments[name] = value
    
    if limit_traceback:
        with _limit_traceback(1):
            return function(**arguments)

    return function(**arguments)


def cut_url(url: str):
    try:
        url = url[:url.index("?")]
    except ValueError:
        pass

    if url.startswith("http://") or url.startswith("https://"):
        return url.replace("//", "", 1)

    return url


def get_extension(url: str):
    url = cut_url(url)

    if url.endswith(".parquet"):
        return "parquet"

    if url.endswith(".csv"):
        return "csv"

    if url.endswith(".pickle"):
        return "pickle"

    print(f"unknown file extension: {url}")
    raise click.Abort()


def _download_head(
    session: requests.Session,
    url: str,
    path: str,
    log: bool,
    print: callable,
):
    logged = False
    response: requests.Response = None

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

        if response is not None:
            response.close()

        raise


def download(
    url: str,
    path: str,
    log=True,
    print=print,
    progress_bar=True,
    max_retry=10,
    session: typing.Optional[requests.Session] = None
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

    with tempfile.TemporaryDirectory(
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

                    with tqdm.tqdm(
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


@contextlib.contextmanager
def limit_traceback(forward=0):
    if forward < 0:
        raise ValueError("forward must be >= 0")

    try:
        yield
    except SystemExit:
        raise
    except BaseException:
        exc_type, exc_value, exc_traceback = sys.exc_info()

        for _ in range(forward + 1):  # +1 to skip "yield"
            exc_traceback = exc_traceback.tb_next

        traceback.print_exception(exc_type, exc_value, exc_traceback)

        sys.exit(1)

_limit_traceback = limit_traceback


def exit_via(error: "api.ApiException", **kwargs):
    print("\n---")
    error.print_helper(**kwargs)
    exit(1)


def timeit(params: list):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs.update(zip(
                func.__code__.co_varnames[:func.__code__.co_argcount],
                args
            ))

            start_time = time.perf_counter()
            try:
                return func(**kwargs)
            finally:
                end_time = time.perf_counter()
                total_time = end_time - start_time

                if params is not None:
                    arguments = ", ".join([
                        str(value) if name in params else "..."
                        for name, value in kwargs.items()
                    ])

                    print(f'[debug] {func.__name__}({arguments}) took {total_time:.4f} seconds', file=sys.stderr)
                else:
                    print(f'[debug] {func.__name__} took {total_time:.4f} seconds', file=sys.stderr)

        return wrapper

    return decorator


timeit_noarg = timeit(None)


def split_at_nans(
    dataframe: pandas.DataFrame,
    column_name: str,
    keep_empty=False,
):
    dataframe = dataframe.reset_index(drop=True)

    indices = dataframe.index[dataframe[column_name].isna()].tolist()
    indices = [-1] + indices + [len(dataframe)]

    parts = []
    for i in range(len(indices) - 1):
        start = indices[i] + 1
        end = indices[i + 1]

        if start != end or keep_empty:
            parts.append(dataframe.iloc[start:end])

    return parts


class Tracer:

    def __init__(self, printer=print):
        self._depth = 0
        self._printer = printer

    @property
    def padding(self):
        return "  " * self._depth

    def loop(
        self,
        iterator: iter,
        action: typing.Union[str, callable],
        value_placeholder="{value}",
    ):
        has_value_placeholder = False
        is_callable = callable(action)
        if not is_callable:
            action = str(action)
            has_value_placeholder = value_placeholder in action

        for value in iterator:
            if is_callable:
                action_message = str(action(value))
            else:
                action_message = action

                if has_value_placeholder:
                    action_message = action_message.replace(value_placeholder, str(value))

            with self.log(action_message):
                yield value

    @contextlib.contextmanager
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

            gc.collect()

            end = datetime.datetime.now()
            self._printer(f"{start} {self.padding} {action} took {end - start}")


def ascii_table(
    headers: typing.List[str],
    rows: typing.List[typing.List[str]]
):
    rows = [
        list(map(str, row))
        for row in rows
    ]

    rows.insert(0, headers)

    max_length_per_columns = [
        max((len(row[index]) for row in rows))
        for index in range(len(rows[0]))
    ]

    for row in rows:
        print("  ", end="")

        for column_index, value in enumerate(row):
            width = max_length_per_columns[column_index] + 3
            print(value.ljust(width), end="")

        print()


class LimitedSizeIO:

    def __init__(
        self,
        underlying_io: io.IOBase,
        limit: int,
        callback: typing.Optional[typing.Callable[[int], None]] = None
    ):
        self.underlying_io = underlying_io
        self.limit = limit
        self.callback = callback
        self.read_so_far = 0

    def read(self, size=-1):
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
