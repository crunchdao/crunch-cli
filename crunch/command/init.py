import os
from dataclasses import dataclass
from typing import Optional, Union

import click

from crunch.api import Auth, Client, SizeVariant
from crunch.repository import Authentication, ProjectInfo, Repository


@dataclass
class CloneTokenSetupCredentials:
    token: str


@dataclass
class ApiKeySetupCredentials:
    competition_name: str
    project_name: str


SetupCredentials = Union[CloneTokenSetupCredentials, ApiKeySetupCredentials]


def _check_if_already_exists(directory: str, force: bool):
    if not os.path.exists(directory):
        return False

    if force:
        return True
    elif len(os.listdir(directory)):
        print(f"init: {directory}: directory not empty (use --force to overwrite)")
        raise click.Abort()


def init(
    *,
    setup_credentials: SetupCredentials,
    directory: str,
    model_directory: str,
    force: bool,
    data_size_variant: SizeVariant = SizeVariant.DEFAULT
) -> Repository:
    should_overwrite = _check_if_already_exists(directory, force)

    plain_token: Optional[str]
    if isinstance(setup_credentials, CloneTokenSetupCredentials):
        client = Client.from_env()
        project_token = client.project_tokens.upgrade(setup_credentials.token)

        project = project_token.project
        authentication = Authentication.PROJECT_TOKEN
        plain_token = project_token.plain
    else:
        auth = Auth.api_key_from_notebook_environment()
        client = Client.from_env(auth=auth)

        project = client.competitions.get(setup_credentials.competition_name).projects.get("@me", setup_credentials.project_name)
        authentication = Authentication.NOTEBOOK_ENVIRONMENT_SECRET_API_KEY
        plain_token = None

    repository = Repository.init(
        directory,
        project=ProjectInfo(
            competition_name=project.competition.name,
            project_name=project.name,
            user_id=project.user_id,
            size_variant=data_size_variant,
            authentication=authentication,
        ),
        project_token=plain_token,
        overwrite=bool(should_overwrite),
    )

    os.chdir(repository.root_directory_path)
    os.makedirs(model_directory, exist_ok=True)

    return repository
