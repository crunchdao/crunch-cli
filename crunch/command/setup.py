import io
import json
import os
import pkgutil
import shutil
import tarfile

import click
import requests

from .. import api, constants, utils


def _check_if_already_exists(directory: str, force: bool):
    if os.path.exists(directory):
        if force:
            print(f"delete {directory}")
            shutil.rmtree(directory)
        else:
            print(f"{directory}: already exists (use --force to override)")
            raise click.Abort()


def _read_demo_file(file_name: str):
    resource = "/".join(["..", "demo-project", file_name])
    return pkgutil.get_data(__package__, resource)


def _setup_demo(directory: str, filter: list = None):
    files = json.loads(_read_demo_file("files.json").decode("utf-8"))
    for file in files:
        if filter and file not in filter:
            continue

        print(f"use {file}")

        content = _read_demo_file(file)

        path = os.path.join(directory, file)
        with open(path, "wb") as fd:
            fd.write(content)


def _setup_submission(directory: str, code_tar: io.BytesIO):
    tar = tarfile.open(fileobj=code_tar)
    for member in tar.getmembers():
        path = os.path.join(directory, member.name)
        print(f"extract {path}")

        os.makedirs(os.path.dirname(path), exist_ok=True)

        fileobj = tar.extractfile(member)
        with open(path, "wb") as fd:
            fd.write(fileobj.read())


def setup(
    session: requests.Session,
    clone_token: str,
    submission_number: str,
    competition_name: str,
    user_login: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
):
    _check_if_already_exists(directory, force)

    push_token = session.post(
        f"/v2/competitions/{competition_name}/projects/{user_login}/tokens",
        json={
            "type": "PERMANENT",
            "cloneToken": clone_token
        }
    ).json()

    dot_crunchdao_path = os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    os.makedirs(dot_crunchdao_path)

    utils.write_project_info(utils.ProjectInfo(
        competition_name,
        user_login
    ), directory)

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(push_token['plain'])

    try:
        code_tar = io.BytesIO(
            session.get(
                f"/v2/competitions/{competition_name}/projects/{user_login}/clone",
                params={
                    "pushToken": push_token['plain'],
                    "submissionNumber": submission_number,
                    "includeModel": not no_model,
                }
            ).content
        )
    except api.NeverSubmittedException:
        _setup_demo(directory)
    else:
        _setup_submission(directory, code_tar)

    path = os.path.join(directory, model_directory)
    os.makedirs(path, exist_ok=True)
