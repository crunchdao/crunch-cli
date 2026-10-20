import os
from dataclasses import dataclass

import click

from crunch.api import Auth, Client, Project, SizeVariant
from crunch.repository import ProjectInfo, Repository

COMPETITION_NAME_PLACEHOLDER = "{competitionName}"
PROJECT_NAME_PLACEHOLDER = "{projectName}"

API_KEY_CLONE_TOKEN = "@api-key"


@dataclass(frozen=True)
class ProjectAccess:
    project: Project
    project_token: str | None


def exchange_clone_token(clone_token: str) -> ProjectAccess:
    client = Client.from_env()
    project_token = client.project_tokens.upgrade(clone_token)

    plain = project_token.plain
    assert plain is not None

    return ProjectAccess(
        project=project_token.project,
        project_token=plain,
    )


def find_project_with_api_key(
    competition_name: str,
    project_name: str,
) -> ProjectAccess:
    client = Client.from_env(auth=Auth.api_key_from_notebook_environment())

    project = client.competitions.get(competition_name).projects.get("@me", project_name)

    return ProjectAccess(
        project=project,
        project_token=None,
    )


def resolve_project_access(
    *,
    clone_token: str,
    competition_name: str,
    project_name: str | None,
) -> ProjectAccess:
    if clone_token != API_KEY_CLONE_TOKEN:
        return exchange_clone_token(clone_token)

    if project_name is None:
        raise click.UsageError(f"a project name must be given with `{API_KEY_CLONE_TOKEN}`")

    return find_project_with_api_key(competition_name, project_name)


def format_directory(
    directory: str,
    competition_name: str,
    project_name: str,
) -> str:
    directory = directory \
        .replace(COMPETITION_NAME_PLACEHOLDER, competition_name) \
        .replace(PROJECT_NAME_PLACEHOLDER, project_name)

    return os.path.normpath(directory)


def _check_directory_is_empty(directory: str, force: bool):
    if not force and os.path.exists(directory) and len(os.listdir(directory)):
        print(f"init: {directory}: directory not empty (use --force to overwrite)")
        raise click.Abort()


def init(
    *,
    clone_token: str,
    competition_name: str,
    project_name: str | None,
    directory: str,
    model_directory: str,
    force: bool,
    data_size_variant: SizeVariant = SizeVariant.DEFAULT
) -> Repository:
    _check_directory_is_empty(directory, force)

    project_access = resolve_project_access(
        clone_token=clone_token,
        competition_name=competition_name,
        project_name=project_name,
    )

    project = project_access.project

    repository = Repository.init(
        directory,
        project=ProjectInfo(
            competition_name=project.competition.name,
            project_name=project.name,
            user_id=project.user_id,
            size_variant=data_size_variant,
        ),
        project_token=project_access.project_token,
        overwrite=force,
    )

    os.chdir(repository.root_directory_path)
    os.makedirs(model_directory, exist_ok=True)

    return repository
