import abc
import os
import types
import typing

import requests

from .. import constants, store

ModuleFileName = typing.Literal["leaderboard", "runner", "scoring", "submission"]


class NoCodeFoundError(RuntimeError):
    pass


class CodeLoadError(ImportError):
    pass


class MissingFunctionError(RuntimeError):
    pass


class ModuleWrapper:

    def __init__(
        self,
        module: types.ModuleType,
    ):
        self._module = module

    def _get_function(self, name: str, ensure: bool):
        function = getattr(self._module, name, None)

        if ensure and function is None:
            raise MissingFunctionError(f"no `{name}` function from module {self._module}")

        return function


class CodeLoader(abc.ABC):

    def load(self):
        location = self.location
        name = os.path.basename(location)

        try:
            module = types.ModuleType(name)
            module.__loader__ = self
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
    @abc.abstractmethod
    def location(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def source(self) -> str:
        pass


class GithubCodeLoader(CodeLoader):

    def __init__(
        self,
        competition_name: str,
        file_name: ModuleFileName,
        repository: str = None,
        branch: str = None,
        user_agent="curl/7.88.1"
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

    def __init__(self, path: str):
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
    file_name: ModuleFileName
):
    return os.path.join(
        "competitions",
        competition_name,
        "scoring",
        f"{file_name}.py"
    ).replace("\\", "/")


def deduce(
    competition_name: str,
    file_name: ModuleFileName,
    github_repository: typing.Optional[str] = None,
    github_branch: typing.Optional[str] = None,
    directory_path: typing.Optional[str] = None,
):
    if not directory_path:
        directory_path = store.competitions_directory_path

    if directory_path:
        path = os.path.join(
            directory_path,
            _format_relative_module_path(competition_name, file_name)
        )

        return LocalCodeLoader(path)

    return GithubCodeLoader(competition_name, file_name, github_repository, github_branch)
