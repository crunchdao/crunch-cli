import os
from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable, Literal, Optional

import crunch.store as store

ModuleFileName = Literal["leaderboard", "runner", "scoring", "submission"]


class NoCodeFoundError(RuntimeError):
    pass


class CodeLoadError(ImportError):
    pass


class MissingFunctionError(RuntimeError):
    pass


class ModuleWrapper:

    def __init__(
        self,
        module: ModuleType,
    ):
        self._module = module

    def _get_function(
        self,
        *,
        name: str,
        ensure: bool,
    ) -> Callable[..., Any]:
        function = getattr(self._module, name, None)

        if ensure and function is None:
            raise MissingFunctionError(f"no `{name}` function from module {self._module}")

        if not callable(function):
            raise MissingFunctionError(f"function `{name}` from module {self._module} is not callable")

        return function


class CodeLoader(ABC):

    def load(self):
        location = self.location
        name = os.path.basename(location)

        try:
            module = ModuleType(name)
            module.__loader__ = self  # type: ignore
            module.__file__ = location
            module.__path__ = [os.path.dirname(location)]
            module.__package__ = name.rpartition('.')[0]

            code = compile(self.source, location, 'exec')
            exec(code, module.__dict__)
        except NoCodeFoundError:
            raise
        except BaseException as exception:
            raise CodeLoadError(f"could not load {location}") from exception

        return module

    @property
    @abstractmethod
    def location(self) -> str:
        pass

    @property
    @abstractmethod
    def source(self) -> str:
        pass


class GithubCodeLoader(CodeLoader):

    def __init__(
        self,
        *,
        competition_name: str,
        file_name: ModuleFileName,
        repository: Optional[str] = None,
        branch: Optional[str] = None,
        user_agent: str = "curl/7.88.1"
    ):
        repository = repository or store.competitions_repository
        branch = branch or store.competitions_branch

        self._url = f"https://github.com/{repository}/raw/refs/heads/{branch}/{_format_relative_module_path(competition_name, file_name)}"
        self._user_agent = user_agent

    @property
    def location(self):
        return self._url

    @property
    def source(self):
        import requests

        response = requests.get(
            self._url,
            headers={
                "User-Agent": self._user_agent
            }
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                raise NoCodeFoundError(f"no code found at url: {self._url}") from error

            raise

        return response.text


class LocalCodeLoader(CodeLoader):

    def __init__(
        self,
        *,
        path: str
    ):
        self.path = path

    @property
    def location(self):
        return self.path

    @property
    def source(self):
        try:
            with open(self.path, "r") as fd:
                return fd.read()
        except FileNotFoundError as error:
            raise NoCodeFoundError(f"no code found at path: {self.path}") from error


def _format_relative_module_path(
    competition_name: str,
    file_name: ModuleFileName,
):
    return os.path.join(
        "competitions",
        competition_name,
        "scoring",
        f"{file_name}.py"
    ).replace("\\", "/")


def deduce(
    *,
    competition_name: str,
    file_name: ModuleFileName,
    github_repository: Optional[str] = None,
    github_branch: Optional[str] = None,
    directory_path: Optional[str] = None,
):
    if not directory_path:
        directory_path = store.competitions_directory_path

    if directory_path:
        path = os.path.join(
            directory_path,
            _format_relative_module_path(competition_name, file_name)
        )

        return LocalCodeLoader(
            path=path,
        )
    else:
        return GithubCodeLoader(
            competition_name=competition_name,
            file_name=file_name,
            repository=github_repository,
            branch=github_branch,
        )
