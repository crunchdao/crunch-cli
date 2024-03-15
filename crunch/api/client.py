import os
import typing
import urllib.parse

import requests

from .. import constants, store, utils
from .auth import ApiKeyAuth, Auth, NoneAuth, PushTokenAuth
from .domain.check import CheckEndpointMixin
from .domain.competition import (CompetitionCollection,
                                 CompetitionEndpointMixin, CompetitionFormat)
from .domain.crunch import CrunchEndpointMixin
from .domain.data_release import DataReleaseEndpointMixin
from .domain.library import LibraryCollection, LibraryEndpointMixin
from .domain.metric import MetricEndpointMixin
from .domain.phase import PhaseEndpointMixin
from .domain.prediction import PredictionEndpointMixin
from .domain.project import (Project, ProjectEndpointMixin,
                             ProjectTokenCollection)
from .domain.quickstarter import (QuickstarterCollection,
                                  QuickstarterEndpointMixin)
from .domain.round import RoundEndpointMixin
from .domain.run import RunEndpointMixin
from .domain.runner import RunnerRun, RunnerRunEndpointMixin
from .domain.score import ScoreEndpointMixin
from .domain.submission import SubmissionEndpointMixin
from .domain.user import UserCollection, UserEndpointMixin
from .errors import convert_error


class EndpointClient(
    requests.Session,
    CheckEndpointMixin,
    CompetitionEndpointMixin,
    CrunchEndpointMixin,
    DataReleaseEndpointMixin,
    LibraryEndpointMixin,
    MetricEndpointMixin,
    PhaseEndpointMixin,
    PredictionEndpointMixin,
    ProjectEndpointMixin,
    QuickstarterEndpointMixin,
    RoundEndpointMixin,
    RunEndpointMixin,
    RunnerRunEndpointMixin,
    ScoreEndpointMixin,
    SubmissionEndpointMixin,
    UserEndpointMixin,
):

    def __init__(
        self,
        base_url: str,
        auth: Auth
    ):
        super().__init__()

        self.base_url = base_url
        self.auth_ = auth

    def request(self, method, url, *args, **kwargs):
        headers = kwargs.pop("headers", {})
        params = kwargs.pop("params", {})
        data = kwargs.pop("data", None)

        self.auth_.apply(headers, params, data)

        return super().request(
            method,
            urllib.parse.urljoin(self.base_url, url),
            *args,
            headers=headers,
            params=params,
            data=data,
            **kwargs,
        )

    def _raise_for_status(
        self,
        response: requests.Response,
    ):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            content = error.response.json()
            converted = convert_error(content)

            raise converted

    def _result(
        self,
        response: requests.Response,
        json=False,
        binary=False
    ):
        assert not (json and binary)
        self._raise_for_status(response)

        if json:
            return response.json()

        if binary:
            return response.content

        return response.text


class Client:

    def __init__(
        self,
        api_base_url: str,
        web_base_url: str,
        auth: Auth
    ):
        self.api = EndpointClient(api_base_url, auth)
        self.web_base_url = web_base_url

    @property
    def competitions(self):
        return CompetitionCollection(client=self)

    @property
    def libraries(self):
        return LibraryCollection(client=self)

    @property
    def users(self):
        return UserCollection(client=self)

    @property
    def project_tokens(self):
        return ProjectTokenCollection(
            competition=None,
            client=self
        )

    def quickstarters(self, competition_format: CompetitionFormat):
        return QuickstarterCollection(
            competition=None,
            competition_format=competition_format,
            client=self
        )

    def get_runner_run(self, run_id: int):
        return RunnerRun(
            run_id,
            client=self
        )

    def format_web_url(self, path: str):
        return urllib.parse.urljoin(
            self.web_base_url,
            path
        )

    @staticmethod
    def from_env(
        auth: typing.Optional[Auth] = None
    ):
        store.load_from_env()

        if auth is None:
            api_key = os.getenv(constants.API_KEY_ENV_VAR)
            if api_key:
                auth = ApiKeyAuth(api_key)
            else:
                auth = NoneAuth()

        return Client(
            store.api_base_url,
            store.web_base_url,
            auth
        )

    @staticmethod
    def from_project() -> typing.Tuple["Client", Project]:
        store.load_from_env()

        project_info = utils.read_project_info()
        push_token = utils.read_token()

        client = Client(
            store.api_base_url,
            store.web_base_url,
            PushTokenAuth(push_token)
        )

        competition = client.competitions.get(project_info.competition_name)
        project = competition.projects.get_reference(None, project_info.user_id)

        return client, project
