import os
import re
from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import parse_qs, urlencode

import requests
from retry import retry

import crunch.store as store
from crunch import constants
from crunch.utils import build_user_agent

ModuleFileName = Literal["leaderboard", "reward", "runner", "scoring", "submission"]


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


class HttpCodeLoader(CodeLoader):

    _url: str
    _user_agent: str

    def __init__(
        self,
        *,
        url: str,
        user_agent: Optional[str] = None,
    ):
        self._url = url
        self._user_agent = user_agent or build_user_agent()

    @property
    def location(self):
        return self._url

    @property
    def source(self):
        return self._fetch()

    @retry(requests.RequestException, tries=3, delay=2, logger=None)
    def _fetch(self):
        response = requests.get(
            self._url,
            headers={
                "User-Agent": self._user_agent,
            }
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                raise NoCodeFoundError(f"no code found at url: {self._url}") from error

            raise

        return response.text

    @staticmethod
    def new_github(
        *,
        competition_name: str,
        file_name: ModuleFileName,
        repository: str,
        reference: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> "HttpCodeLoader":
        url = format_github_url(
            repository=repository,
            reference=reference or "master",
            competition_name=competition_name,
            file_name=file_name,
        )

        return HttpCodeLoader(
            url=url,
            user_agent=user_agent,
        )

    @staticmethod
    def new_cache(
        *,
        competition_name: str,
        file_name: ModuleFileName,
        base_url: str,
        schema: Optional[str] = None,
        reference: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> "HttpCodeLoader":
        if schema is None:
            schema = "https"
        elif schema not in ("http", "https"):
            raise ValueError(f"unsupported schema: {schema}")

        url = f"{schema}://{base_url.rstrip('/')}/{format_relative_module_path(competition_name, file_name)}"

        if reference:
            query = urlencode({"reference": reference})
            url += f"?{query}"

        return HttpCodeLoader(
            url=url,
            user_agent=user_agent,
        )


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

    @staticmethod
    def new_directory(
        *,
        competition_name: str,
        file_name: ModuleFileName,
        competitions_path: str,
    ):
        path = os.path.join(
            competitions_path,
            format_relative_module_path(competition_name, file_name)
        )

        return LocalCodeLoader(
            path=path,
        )


def format_relative_module_path(
    competition_name: str,
    file_name: ModuleFileName,
):
    return os.path.join(
        "competitions",
        competition_name,
        "scoring",
        f"{file_name}.py"
    ).replace("\\", "/")


def format_github_url(
    repository: str,
    reference: str,
    competition_name: str,
    file_name: ModuleFileName,
):
    return f"https://github.com/{repository}/raw/refs/heads/{reference}/{format_relative_module_path(competition_name, file_name)}"


def deduce(
    *,
    competition_name: str,
    file_name: ModuleFileName,
    source: Optional[str] = None,
):
    if source is None:
        source = store.competitions_source or constants.COMPETITIONS_SOURCE_CACHE_DEFAULT

    if "://" not in source:
        raise ValueError(f"invalid source: {source}")

    schema, source = source.split("://", 1)

    if "?" in source:
        source, params_string = source.split("?", 1)
        params = parse_qs(params_string)
    else:
        params = {}

    if "+" in schema:
        schema, schema2 = schema.split("+", 1)
    else:
        schema2 = None

    if schema == "local":
        directory_path = source
        return LocalCodeLoader.new_directory(
            competition_name=competition_name,
            file_name=file_name,
            competitions_path=directory_path,
        )

    elif schema == "github":
        github_repository = source
        if not re.match(r"^[\w-]+/[\w-]+$", github_repository):
            raise ValueError(f"invalid github repository: {github_repository}")

        return HttpCodeLoader.new_github(
            competition_name=competition_name,
            file_name=file_name,
            repository=github_repository,
            reference=_get_reference(params),
        )

    elif schema == "cache":
        base_url = source

        return HttpCodeLoader.new_cache(
            competition_name=competition_name,
            file_name=file_name,
            base_url=base_url,
            schema=schema2,
            reference=_get_reference(params),
        )

    else:
        raise ValueError(f"unsupported schema: {schema}")


def _get_reference(params: Dict[str, List[str]]) -> Optional[str]:
    reference, = params.get("reference") or [None]
    return reference
