import os
import shutil
import typing

import click

from .. import api, command, constants, utils


def _delete_tree_if_exists(path: str):
    if os.path.exists(path):
        print(f"delete {path}")
        shutil.rmtree(path)


def _check_if_already_exists(directory: str, force: bool):
    if not os.path.exists(directory):
        return False

    if force:
        return True
    elif len(os.listdir(directory)):
        print(f"{directory}: directory not empty (use --force to override)")
        raise click.Abort()


def setup(
    clone_token: str,
    submission_number: str,
    competition_name: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
):
    should_delete = _check_if_already_exists(directory, force)

    client = api.Client.from_env()
    project_token = client.project_tokens.upgrade(clone_token)

    dot_crunchdao_path = os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    if should_delete:
        _delete_tree_if_exists(dot_crunchdao_path)

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
        raise api.NeverSubmittedException("qsd")
        urls = project.clone(
            submission_number=submission_number,
            include_model=not no_model,
        )

        for path, url in urls.items():
            utils.download(url, path)
    except api.NeverSubmittedException:
        command.quickstarter(
            quickstarter_name,
            show_notebook_quickstarters,
            True,
        )

    os.makedirs(model_directory, exist_ok=True)
