import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Optional

import click

from crunch.api import Auth, Client, Project, SizeVariant
from crunch.repository import Authentication, ProjectInfo, Repository

if TYPE_CHECKING:
    from crunch.api import ProjectIdentifierType


@dataclass(frozen=True)
class ProjectAccess:
    project: Project
    authentication: Authentication
    auth: Auth
    project_token: Optional[str]


class SetupCredentials(ABC):

    @cached_property
    @abstractmethod
    def project_name(self) -> str:
        ...

    @cached_property
    @abstractmethod
    def access(self) -> ProjectAccess:
        ...


class CloneTokenSetupCredentials(SetupCredentials):

    def __init__(
        self,
        token: str,
        project_name: Optional[str] = None,
    ):
        self.token = token
        self._project_name = project_name

    @cached_property
    def project_name(self) -> str:
        if self._project_name is None:
            raise ValueError("the project of a clone token is only known once the token is used")

        return self._project_name

    @cached_property
    def access(self) -> ProjectAccess:
        client = Client.from_env()
        project_token = client.project_tokens.upgrade(self.token)

        plain = project_token.plain
        assert plain is not None

        return ProjectAccess(
            project=project_token.project,
            authentication=Authentication.PROJECT_TOKEN,
            auth=Auth.project_token(plain),
            project_token=plain,
        )


class ApiKeySetupCredentials(SetupCredentials):

    def __init__(
        self,
        competition_name: str,
        project_identifier: "ProjectIdentifierType" = "@first",
    ):
        self.competition_name = competition_name
        self.project_identifier = project_identifier

    @cached_property
    def project_name(self) -> str:
        return self.access.project.name

    @cached_property
    def access(self) -> ProjectAccess:
        auth = Auth.api_key_from_notebook_environment()
        client = Client.from_env(auth=auth)

        project = client.competitions.get(self.competition_name).projects.get("@me", self.project_identifier)

        return ProjectAccess(
            project=project,
            authentication=Authentication.NOTEBOOK_ENVIRONMENT_SECRET_API_KEY,
            auth=auth,
            project_token=None,
        )


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

    access = setup_credentials.access
    project = access.project

    repository = Repository.init(
        directory,
        project=ProjectInfo(
            competition_name=project.competition.name,
            project_name=project.name,
            user_id=project.user_id,
            size_variant=data_size_variant,
            authentication=access.authentication,
        ),
        project_token=access.project_token,
        auth=access.auth,
        overwrite=bool(should_overwrite),
    )

    os.chdir(repository.root_directory_path)
    os.makedirs(model_directory, exist_ok=True)

    return repository
