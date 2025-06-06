import os
import shutil

import click

from .. import api, constants, utils


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


def init(
    *,
    clone_token: str,
    directory: str,
    model_directory: str,
    force: bool,
    data_size_variant=api.SizeVariant.DEFAULT
):
    should_delete = _check_if_already_exists(directory, force)

    client = api.Client.from_env()
    project_token = client.project_tokens.upgrade(clone_token)

    dot_crunchdao_path = os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    if should_delete:
        _delete_tree_if_exists(dot_crunchdao_path)

    os.makedirs(dot_crunchdao_path, exist_ok=True)

    plain = project_token.plain
    project = project_token.project

    project_info = utils.ProjectInfo(
        project.competition.name,
        project.name,
        project.user_id,
        data_size_variant,
    )

    utils.write_project_info(project_info, directory)
    utils.write_token(plain, directory)

    os.chdir(directory)
    os.makedirs(model_directory, exist_ok=True)
