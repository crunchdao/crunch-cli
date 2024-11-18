import abc
import os
import types

import requests

from .. import constants


class CodeLoadError(ImportError):
    pass


class CodeLoader(abc.ABC):

    def load(self):
        name = "scoring.py"
        path = self.path

        try:
            module = types.ModuleType(name)
            module.__loader__ = self
            module.__file__ = path
            module.__path__ = [os.path.dirname(path)]
            module.__package__ = name.rpartition('.')[0]

            code = compile(self.source, path, 'exec')
            exec(code, module.__dict__)
        except BaseException as exception:
            raise CodeLoadError(f"could not load {path}") from exception

        return module

    @property
    @abc.abstractmethod
    def path(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def source(self) -> str:
        pass


class GithubCodeLoader(CodeLoader):

    def __init__(
        self,
        competition_name: str,
        repository=constants.COMPETITIONS_REPOSITORY,
        branch=constants.COMPETITIONS_BRANCH,
        user_agent="curl/7.88.1"
    ):
        self._path = f"https://raw.githubusercontent.com/{repository}/refs/heads/{branch}/competitions/{competition_name}/scoring/scoring.py"
        self._user_agent = user_agent

    @property
    def path(self):
        return self._path

    @property
    def source(self):
        response = requests.get(
            self._path,
            headers={
                "User-Agent": self._user_agent
            }
        )

        response.raise_for_status()
        return response.text


class LocalCodeLoader(CodeLoader):

    def __init__(self, path: str):
        self._path = path

    @property
    def path(self):
        return self._path

    @property
    def source(self):
        with open(self._path, "r") as fd:
            return fd.read()
