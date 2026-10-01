import os

import click

from crunch.api import Client, SizeVariant
from crunch.repository import ProjectInfo, Repository


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
    data_size_variant: SizeVariant = SizeVariant.DEFAULT
) -> Repository:
    should_overwrite = _check_if_already_exists(directory, force)

    client = Client.from_env()
    project_token = client.project_tokens.upgrade(clone_token)

    project = project_token.project

    repository = Repository.init(
        directory,
        project=ProjectInfo(
            competition_name=project.competition.name,
            project_name=project.name,
            user_id=project.user_id,
            size_variant=data_size_variant,
        ),
        push_token=project_token.plain,
        overwrite=bool(should_overwrite),
    )

    os.chdir(repository.root_directory_path)
    os.makedirs(model_directory, exist_ok=True)

    return repository
