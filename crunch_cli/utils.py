
import requests
import urllib
import re
import json
import traceback
import click
import os

from . import constants


class CustomSession(requests.Session):
    # https://stackoverflow.com/a/51026159/7292958

    def __init__(self, base_url=None, debug=False):
        super().__init__()
        self.base_url = base_url
        self.debug = debug

    def request(self, method, url, *args, **kwargs):
        response = super().request(
            method,
            urllib.parse.urljoin(self.base_url, url),
            *args,
            **kwargs
        )

        status_code = response.status_code
        if status_code != 200:
            print(f"{method} {url}: {status_code}")

            try:
                print(json.dumps(response.json(), indent=4))
            except:
                print(response.text)

            if self.debug:
                traceback.print_stack()

            raise click.Abort()

        return response


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


def _read_crunchdao_file(name: str):
    path = os.path.join(constants.DOT_CRUNCHDAO_DIRECTORY, name)
    if not os.path.exists(path):
        print(f"{path}: not found")
        raise click.Abort()

    with open(path) as fd:
        return fd.read()


def read_project_name():
    return _read_crunchdao_file(constants.PROJECT_FILE)


def read_token():
    return _read_crunchdao_file(constants.TOKEN_FILE)
