import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import crunch.store as store
from crunch.api import Client, Project, SizeVariant
from crunch.api._auth import ApiKeyAuth, Auth, PushTokenAuth
from crunch.constants import DOT_CRUNCH_DIRECTORY, DOT_DATA_DIRECTORY, DOT_PREDICTION_DIRECTORY, PROJECT_FILE, TOKEN_FILE, UPLOAD_CACHE_FILE

__all__ = [
    "Authentication",
    "ProjectInfo",
    "Repository",
    "RepositoryError",
    "RepositoryNotFoundError",
    "RepositoryAlreadyExistsError",
    "RepositoryFileNotFoundError",
    "RepositoryFileInvalidError",
]


class RepositoryError(Exception):
    pass


class RepositoryNotFoundError(RepositoryError):

    def __init__(self, directory_path: str):
        super().__init__(f"no {DOT_CRUNCH_DIRECTORY} directory found in {directory_path} or any of its parents, make sure to `cd <competition>` first")

        self.directory_path = directory_path


class RepositoryAlreadyExistsError(RepositoryError):

    def __init__(self, dot_crunch_directory_path: str):
        super().__init__(f"{dot_crunch_directory_path}: already exists")

        self.dot_crunch_directory_path = dot_crunch_directory_path


class RepositoryFileNotFoundError(RepositoryError):

    def __init__(self, file_path: str):
        super().__init__(f"{file_path}: not found, the project seems corrupted, try to setup it again")

        self.file_path = file_path


class RepositoryFileInvalidError(RepositoryError):

    def __init__(self, file_path: str, cause: Exception):
        super().__init__(f"{file_path}: could not be read ({cause.__class__.__name__}: {cause}), the project seems corrupted, try to setup it again")

        self.file_path = file_path
        self.cause = cause


class Authentication(Enum):
    PUSH_TOKEN = "PUSH_TOKEN"
    NOTEBOOK_ENVIRONMENT_SECRET_API_KEY = "NOTEBOOK_ENVIRONMENT_SECRET_API_KEY"


@dataclass(frozen=True)
class ProjectInfo:
    competition_name: str
    project_name: str
    user_id: int
    size_variant: SizeVariant
    authentication: Authentication = Authentication.PUSH_TOKEN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "competitionName": self.competition_name,
            "projectName": self.project_name,
            "userId": self.user_id,
            "sizeVariant": self.size_variant.name,
            "authentication": self.authentication.name,
        }

    @staticmethod
    def from_dict(root: Dict[str, Any]) -> "ProjectInfo":
        try:
            size_variant = SizeVariant[root["sizeVariant"]]
        except (KeyError, TypeError):
            size_variant = SizeVariant.DEFAULT

        return ProjectInfo(
            competition_name=root["competitionName"],
            project_name=root.get("projectName") or "default",  # backward compatibility
            user_id=root["userId"],
            size_variant=size_variant,
            authentication=Authentication[root.get("authentication") or Authentication.PUSH_TOKEN.name],  # backward compatibility
        )


class Repository:

    def __init__(
        self,
        root_directory_path: str,
        project: ProjectInfo,
        push_token: Optional[str],
    ):
        self._root_directory_path = os.path.abspath(root_directory_path)
        self._project = project
        self._push_token = push_token
        self._auth: Optional[Auth] = None

    @property
    def root_directory_path(self) -> str:
        return self._root_directory_path

    @property
    def dot_crunch_directory_path(self) -> str:
        return os.path.join(self._root_directory_path, DOT_CRUNCH_DIRECTORY)

    @property
    def data_directory_path(self) -> str:
        return os.path.join(self._root_directory_path, DOT_DATA_DIRECTORY)

    @property
    def prediction_directory_path(self) -> str:
        return os.path.join(self._root_directory_path, DOT_PREDICTION_DIRECTORY)

    @property
    def project_file_path(self) -> str:
        return os.path.join(self.dot_crunch_directory_path, PROJECT_FILE)

    @property
    def push_token_file_path(self) -> str:
        return os.path.join(self.dot_crunch_directory_path, TOKEN_FILE)

    @property
    def upload_cache_file_path(self) -> str:
        return os.path.join(self.dot_crunch_directory_path, UPLOAD_CACHE_FILE)

    def get_project(self) -> ProjectInfo:
        return self._project

    def write_project(self, project: ProjectInfo) -> None:
        _write_file_atomically(self.project_file_path, json.dumps(project.to_dict()))

        self._project = project

    def update_project(
        self,
        *,
        project_name: Optional[str] = None,
        user_id: Optional[int] = None,
        size_variant: Optional[SizeVariant] = None,
        authentication: Optional[Authentication] = None,
    ) -> ProjectInfo:
        current = self._project

        project = ProjectInfo(
            competition_name=current.competition_name,
            project_name=project_name if project_name is not None else current.project_name,
            user_id=user_id if user_id is not None else current.user_id,
            size_variant=size_variant if size_variant is not None else current.size_variant,
            authentication=authentication if authentication is not None else current.authentication,
        )

        self.write_project(project)
        self._auth = None

        return project

    def get_push_token(self) -> Optional[str]:
        return self._push_token

    def write_push_token(self, plain_push_token: str) -> None:
        _write_file_atomically(self.push_token_file_path, plain_push_token)

        self._push_token = plain_push_token
        self._auth = None

    def read_upload_cache(self) -> Optional[Any]:
        path = self.upload_cache_file_path
        if not os.path.exists(path):
            return None

        with open(path) as file:
            return json.load(file)

    def write_upload_cache(self, content: Any) -> None:
        _write_file_atomically(self.upload_cache_file_path, json.dumps(content))

    def create_client(
        self,
        *,
        show_progress: bool = True,
    ) -> Tuple[Client, Project]:
        store.load_from_env()

        client = Client(
            store.api_base_url,
            store.web_base_url,
            self._get_auth(),
            show_progress=show_progress,
        )

        competition = client.competitions.get(self._project.competition_name)
        project = competition.projects.get_reference(None, (self._project.user_id, self._project.project_name))  # pyright: ignore[reportUnknownMemberType]

        return client, project

    def _get_auth(self) -> Auth:
        if self._auth is None:
            self._auth = self._create_auth()

        return self._auth

    def _create_auth(self) -> Auth:
        authentication = self._project.authentication

        if authentication == Authentication.PUSH_TOKEN:
            if self._push_token is None:
                raise RepositoryFileNotFoundError(self.push_token_file_path)

            return PushTokenAuth(self._push_token)

        if authentication == Authentication.NOTEBOOK_ENVIRONMENT_SECRET_API_KEY:
            return ApiKeyAuth.from_notebook_environment()

        raise ValueError(f"unsupported authentication: {authentication}")

    @staticmethod
    def open(
        directory_path: str = ".",
        *,
        search_parents: bool = True,
    ) -> "Repository":
        root_directory_path = _find_root_directory_path(directory_path, search_parents)
        dot_crunch_directory_path = os.path.join(root_directory_path, DOT_CRUNCH_DIRECTORY)

        project_file_path = os.path.join(dot_crunch_directory_path, PROJECT_FILE)

        try:
            project = ProjectInfo.from_dict(json.loads(_read_file(project_file_path)))
        except (ValueError, KeyError) as error:
            raise RepositoryFileInvalidError(project_file_path, error) from error

        push_token = (
            _read_file(os.path.join(dot_crunch_directory_path, TOKEN_FILE))
            if project.authentication == Authentication.PUSH_TOKEN
            else None
        )

        return Repository(root_directory_path, project, push_token)

    @staticmethod
    def try_open(
        directory_path: str = ".",
        *,
        search_parents: bool = True,
    ) -> Optional["Repository"]:
        try:
            return Repository.open(directory_path, search_parents=search_parents)
        except RepositoryError:
            return None

    @staticmethod
    def init(
        directory_path: str,
        *,
        project: ProjectInfo,
        push_token: Optional[str],
        overwrite: bool = False,
    ) -> "Repository":
        if (project.authentication == Authentication.PUSH_TOKEN) != (push_token is not None):
            raise ValueError(f"a push token must be provided if and only if the authentication is {Authentication.PUSH_TOKEN.name}")

        repository = Repository(directory_path, project, push_token)

        dot_crunch_directory_path = repository.dot_crunch_directory_path
        if os.path.exists(dot_crunch_directory_path):
            if not overwrite:
                raise RepositoryAlreadyExistsError(dot_crunch_directory_path)

            shutil.rmtree(dot_crunch_directory_path)

        os.makedirs(dot_crunch_directory_path)
        os.makedirs(repository.data_directory_path, exist_ok=True)
        os.makedirs(repository.prediction_directory_path, exist_ok=True)

        repository.write_project(project)

        if push_token is not None:
            repository.write_push_token(push_token)

        return repository


def _find_root_directory_path(
    directory_path: str,
    search_parents: bool,
) -> str:
    start_directory_path = os.path.abspath(directory_path)

    current_directory_path = start_directory_path
    while True:
        if os.path.isdir(os.path.join(current_directory_path, DOT_CRUNCH_DIRECTORY)):
            return current_directory_path

        parent_directory_path = os.path.dirname(current_directory_path)
        if not search_parents or parent_directory_path == current_directory_path:
            raise RepositoryNotFoundError(start_directory_path)

        current_directory_path = parent_directory_path


def _read_file(path: str) -> str:
    if not os.path.exists(path):
        raise RepositoryFileNotFoundError(path)

    with open(path) as file:
        return file.read()


def _write_file_atomically(path: str, content: str) -> None:
    directory_path = os.path.dirname(path)
    os.makedirs(directory_path, exist_ok=True)

    file_descriptor, temporary_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.",
        dir=directory_path,
    )

    try:
        with os.fdopen(file_descriptor, "w") as file:
            file.write(content)

        os.replace(temporary_path, path)
    except BaseException:
        os.unlink(temporary_path)
        raise
