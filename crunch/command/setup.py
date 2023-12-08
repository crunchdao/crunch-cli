import io
import json
import os
import pkgutil
import shutil
import tarfile

import click
import requests

from .. import api, constants, utils


def _dot_crunchdao(directory: str):
    return os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)


def _delete_tree_if_exists(path: str):
    if os.path.exists(path):
        print(f"delete {path}")
        shutil.rmtree(path)


def _check_if_already_exists(directory: str, force: bool):
    if not os.path.exists(directory):
        return

    if force:
        dot_crunchdao_path = _dot_crunchdao(directory)
        _delete_tree_if_exists(dot_crunchdao_path)
    elif len(os.listdir(directory)):
        print(f"{directory}: directory not empty (use --force to override)")
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
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
):
    _check_if_already_exists(directory, force)

    try:
        push_token = session.post(
            f"/v2/project-tokens/upgrade",
            json={
                "cloneToken": clone_token
            }
        ).json()
    except api.InvalidProjectTokenException:
        print("your token seems to have expired or is invalid")
        print("---")
        print("please follow this link to copy and paste your new setup command:")
        print(session.format_web_url(
            f'/competitions/{competition_name}/submit'
        ))
        print("")

        raise click.Abort()

    dot_crunchdao_path = _dot_crunchdao(directory)
    os.makedirs(dot_crunchdao_path, exist_ok=True)

    project_info = utils.ProjectInfo(
        competition_name,
        push_token["project"]["userId"]
    )

    utils.write_project_info(project_info, directory)

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(push_token['plain'])

    try:
        code_tar = io.BytesIO(
            session.get(
                f"/v2/competitions/{competition_name}/projects/{project_info.user_id}/clone",
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
