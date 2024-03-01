import json
import os
import pkgutil
import shutil

import click

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


def _setup_demo(filter: list = None):
    files = json.loads(_read_demo_file("files.json").decode("utf-8"))
    for file in files:
        if filter and file not in filter:
            continue

        print(f"use {file}")

        content = _read_demo_file(file)
        with open(file, "wb") as fd:
            fd.write(content)


def _setup_submission(urls: dict):
    for path, url in urls.items():
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        utils.download(url, path)


def setup(
    clone_token: str,
    submission_number: str,
    competition_name: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
):
    _check_if_already_exists(directory, force)

    client = api.Client.from_env()

    project_token = client.project_tokens.upgrade(clone_token)

    dot_crunchdao_path = _dot_crunchdao(directory)
    os.makedirs(dot_crunchdao_path, exist_ok=True)

    plain = project_token.plain
    user_id = project_token.project.user_id

    project_info = utils.ProjectInfo(
        competition_name,
        user_id
    )

    utils.write_project_info(project_info, directory)
    utils.write_token(plain, directory)

    os.chdir(directory)
    _, project = api.Client.from_project()

    try:
        urls = project.clone(
            submission_number=submission_number,
            include_model=not no_model,
        )
    except api.NeverSubmittedException:
        _setup_demo(directory)
    else:
        _setup_submission(urls)

    os.makedirs(model_directory, exist_ok=True)
