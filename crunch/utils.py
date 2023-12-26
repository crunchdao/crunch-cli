import dataclasses
import inspect
import json
import logging
import os
import re
import traceback
import typing
import urllib
import urllib.parse

import click
import joblib
import packaging.version
import pandas
import psutil
import requests
import tqdm

from . import api, constants


class CustomSession(requests.Session):
    # https://stackoverflow.com/a/51026159/7292958

    def __init__(self, web_base_url=None, api_base_url=None, debug=False):
        super().__init__()
        self.web_base_url = web_base_url
        self.api_base_url = api_base_url
        self.debug = debug

    def request(self, method, url, *args, **kwargs):
        response = super().request(
            method,
            urllib.parse.urljoin(self.api_base_url, url),
            *args,
            **kwargs
        )

        status_code = response.status_code
        if status_code != 200:
            try:
                error = response.json()
            except:
                print(response.text)
            else:
                code = error.get("code", "")
                message = error.get("message", "")

                if code == "INVALID_PROJECT_TOKEN":
                    raise api.InvalidProjectTokenException(message)
                elif code == "NEVER_SUBMITTED":
                    raise api.NeverSubmittedException(message)
                elif code == "CURRENT_CRUNCH_NOT_FOUND":
                    raise api.CurrentCrunchNotFoundException(message)
                else:
                    print(f"{method} {url}: {status_code}")
                    print(json.dumps(error, indent=4))

            if self.debug:
                traceback.print_stack()

            raise click.Abort()

        return response

    def format_web_url(self, path: str):
        return urllib.parse.urljoin(
            self.web_base_url,
            path
        )


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
            "userId": info.user_id,
        }, fd)


def read_project_info() -> ProjectInfo:
    old_content = _read_crunchdao_file(constants.OLD_PROJECT_FILE, False)
    if old_content is not None:
        return ProjectInfo(
            "adialab",
            root["userId"],
        )

    content = _read_crunchdao_file(constants.PROJECT_FILE, True)
    root = json.loads(content)

    return ProjectInfo(
        root["competitionName"],
        root["userId"],
    )


def read_token():
    return _read_crunchdao_file(constants.TOKEN_FILE)


def read(path: str, dataframe=True, kwargs={}) -> typing.Union[pandas.DataFrame, typing.Any]:
    if dataframe:
        if path.endswith(".parquet"):
            return pandas.read_parquet(path, **kwargs)
        return pandas.read_csv(path, **kwargs)

    return joblib.load(path)


def write(dataframe: typing.Union[pandas.DataFrame, typing.Any], path: str, kwargs={}) -> None:
    if isinstance(dataframe, pandas.DataFrame):
        if path.endswith(".parquet"):
            dataframe.to_parquet(path, **kwargs)
        else:
            dataframe.to_csv(path, **kwargs)
    else:
        joblib.dump(dataframe, path)


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
    try:
        packaging.version.Version(input)
        return True
    except:
        return False


def get_process_memory() -> int:
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


def smart_call(function: callable, default_values: dict, specific_values={}):
    values = {
        **default_values,
        **specific_values
    }

    def warn(message: str):
        logging.warn(f"{function.__name__}: {message}")

    def debug(message: str):
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


def download(url: str, path: str, log=True):
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

                print(
                    f"download {path} from {cut_url(url)} ({file_length_str})"
                )
                logged = True

            with open(path, 'wb') as fd, tqdm.tqdm(total=file_length, unit='iB', unit_scale=True, leave=False) as progress:
                for chunk in response.iter_content(chunk_size=8192):
                    progress.update(len(chunk))
                    fd.write(chunk)
    except:
        if log and not logged:
            print(f"downloading {path} from {cut_url(url)}")

        raise
