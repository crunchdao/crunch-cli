import os
from dataclasses import dataclass
from typing import Optional, Union

import click

from crunch.api import Client, SizeVariant
from crunch.api._auth import ApiKeyAuth
from crunch.repository import Authentication, ProjectInfo, Repository


@dataclass
class CloneTokenSetupMode:
    token: str


@dataclass
class ApiKeySetupMode:
    competition_name: str
    project_name: str


SetupMode = Union[CloneTokenSetupMode, ApiKeySetupMode]


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
    setup_mode: SetupMode,
    directory: str,
    model_directory: str,
    force: bool,
    data_size_variant: SizeVariant = SizeVariant.DEFAULT
) -> Repository:
    should_overwrite = _check_if_already_exists(directory, force)

    push_token: Optional[str]
    if isinstance(setup_mode, CloneTokenSetupMode):
        client = Client.from_env()
        project_token = client.project_tokens.upgrade(setup_mode.token)

        project = project_token.project
        authentication = Authentication.PUSH_TOKEN
        push_token = project_token.plain
    else:
        auth = ApiKeyAuth.from_notebook_environment()
        client = Client.from_env(auth=auth)

        project = client.competitions.get(setup_mode.competition_name).projects.get("@me", setup_mode.project_name)
        authentication = Authentication.NOTEBOOK_ENVIRONMENT_SECRET_API_KEY
        push_token = None

    repository = Repository.init(
        directory,
        project=ProjectInfo(
            competition_name=project.competition.name,
            project_name=project.name,
            user_id=project.user_id,
            size_variant=data_size_variant,
            authentication=authentication,
        ),
        push_token=push_token,
        overwrite=bool(should_overwrite),
    )

    os.chdir(repository.root_directory_path)
    os.makedirs(model_directory, exist_ok=True)

    return repository
