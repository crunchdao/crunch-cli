import io
import json
import os
import pkgutil
import shutil
import tarfile

import click
import requests

from .. import api, constants


def _check_if_already_exists(directory: str, force: bool):
    if os.path.exists(directory):
        if force:
            print(f"delete {directory}")
            shutil.rmtree(directory)
        else:
            print(f"{directory}: already exists (use --force to override)")
            raise click.Abort()


def _setup_demo(directory: str):
    def read_data(file_name):
        resource = "/".join(["..", "demo-project", file_name])
        return pkgutil.get_data(__package__, resource)

    files = json.loads(read_data("files.json").decode("utf-8"))
    for file in files:
        print(f"use {file}")

        content = read_data(file)

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
    project_name: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
):
    _check_if_already_exists(directory, force)

    push_token = session.post(f"/v1/projects/{project_name}/tokens", json={
        "type": "PERMANENT",
        "cloneToken": clone_token
    }).json()

    dot_crunchdao_path = os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    os.makedirs(dot_crunchdao_path)

    project_file_path = os.path.join(dot_crunchdao_path, constants.PROJECT_FILE)
    with open(project_file_path, "w") as fd:
        fd.write(project_name)

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(push_token['plain'])

    try:
        code_tar = io.BytesIO(
            session.get(f"/v1/projects/{project_name}/clone", params={
                "pushToken": push_token['plain'],
                "submissionNumber": submission_number,
                "includeModel": not no_model,
            }).content
        )
    except api.NeverSubmittedException:
        _setup_demo(directory)
    else:
        _setup_submission(directory, code_tar)

    path = os.path.join(directory, model_directory)
    os.makedirs(path, exist_ok=True)
