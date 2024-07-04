import dataclasses
import inspect
import json
import logging
import os
import re
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
            print(f"found project: {current}")
            return

        os.chdir("../")
        if current == os.getcwd():
            print("no project found")
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

    # TODO: need of a better system for handling file versions
    return ProjectInfo(
        root["competitionName"],
        root.get("projectName") or "default",  # backward compatibility
        root["userId"],
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


def to_unix_path(input: str):
    return input\
        .replace("\\", "/")\
        .replace("//", "/")


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
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    suffix_index = 0

    while bytes >= 1024 and suffix_index < 8:
        bytes /= 1024
        suffix_index += 1

    return f"{bytes:,.2f} {suffixes[suffix_index]}"


class _undefined:
    pass


def smart_call(
    function: callable,
    default_values: dict,
    specific_values={},
    log=True
):
    values = {
        **default_values,
        **specific_values
    }

    def warn(message: str):
        if log:
            logging.warn(f"{function.__name__}: {message}")

    def debug(message: str):
        if log:
            logging.debug(f"{function.__name__}: {message}")

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

    return function(**arguments)


def cut_url(url: str):
    try:
        return url[:url.index("?")]
    except ValueError:
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


def download(
    url: str,
    path: str,
    log=True,
    print=print,
    progress_bar=True,
):
    url_cut = cut_url(url)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    logged = False

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            file_length = response.headers.get("Content-Length", None)
            file_length = int(file_length) if not None else None

            if log:
                if file_length is not None:
                    file_length_str = f"{file_length} bytes"
                else:
                    file_length_str = "unknown length"

                print(f"download {path} from {url_cut} ({file_length_str})")
                logged = True

            progress = tqdm.tqdm(
                total=file_length,
                unit='iB',
                unit_scale=True,
                leave=False,
                disable=not progress_bar
            )

            with open(path, 'wb') as fd, progress:
                for chunk in response.iter_content(chunk_size=8192):
                    progress.update(len(chunk))
                    fd.write(chunk)
    except:
        if log and not logged:
            print(f"downloading {path} from {url_cut}")

        raise


def exit_via(error: "api.ApiException", **kwargs):
    print("\n---")
    error.print_helper(**kwargs)
    exit(1)
