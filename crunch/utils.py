import json
import os
import traceback
import typing
import urllib
import urllib.parse
import re

import click
import joblib
import pandas
import requests

from . import constants, api


class CustomSession(requests.Session):
    # https://stackoverflow.com/a/51026159/7292958

    def __init__(self, web_base_url=None, api_base_url=None, debug=False, notebook=False):
        super().__init__()
        self.web_base_url = web_base_url
        self.api_base_url = api_base_url
        self.debug = debug
        self.notebook = notebook

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

                if code == "INVALID_PROJECT_TOKEN" and message == "invalid project token":
                    print("your token seems to have expired or is invalid")
                    self.print_recopy_command()
                elif code == "ENTITY_NOT_FOUND" and message.startswith("no user found with username"):
                    print("user not found, did you rename yourself?")
                    self.print_recopy_command()
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

    def print_recopy_command(self):
        tab = "notebook" if self.notebook else "cli"

        print("---")
        print("please follow this link to copy and paste your new setup command:")
        print(self.format_web_url(f'/submit?tab={tab}'))
        print("")


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


def _read_crunchdao_file(name: str, raise_if_missing: bool):
    path = os.path.join(constants.DOT_CRUNCHDAO_DIRECTORY, name)

    if not os.path.exists(path):
        if raise_if_missing:
            print(f"{path}: not found, are you in a project directory?")
            raise click.Abort()

        return None

    with open(path) as fd:
        return fd.read()


def read_project_name():
    return _read_crunchdao_file(constants.PROJECT_FILE, True)


def read_token(raise_if_missing=False):
    return _read_crunchdao_file(constants.TOKEN_FILE, raise_if_missing)


def read(path: str, dataframe=True) -> typing.Union[pandas.DataFrame, typing.Any]:
    if dataframe:
        if path.endswith(".parquet"):
            return pandas.read_parquet(path)
        return pandas.read_csv(path)

    return joblib.load(path)


def write(dataframe: typing.Union[pandas.DataFrame, typing.Any], path: str) -> None:
    if isinstance(dataframe, pandas.DataFrame):
        if path.endswith(".parquet"):
            dataframe.to_parquet(path)
        else:
            dataframe.to_csv(path)

    joblib.dump(dataframe, path)


def guess_extension(dataframe: typing.Union[pandas.DataFrame, typing.Any]):
    if isinstance(dataframe, pandas.DataFrame):
        return "parquet"

    return "joblib"


def strip_python_special_lines(lines: typing.List[str]):
    return "\n".join(
        line
        for line in lines
        if not re.match(r"^\s*?(!|%|#)", line)
    )
