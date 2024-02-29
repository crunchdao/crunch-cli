import os
import urllib.parse

import requests

from .. import constants, store, utils
from .auth import ApiKeyAuth, Auth, NoneAuth, PushTokenAuth
from .domain.check import CheckEndpointMixin
from .domain.competition import CompetitionCollection, CompetitionEndpointMixin
from .domain.crunch import CrunchEndpointMixin
from .domain.data_release import DataReleaseEndpointMixin
from .domain.phase import PhaseEndpointMixin
from .domain.prediction import PredictionEndpointMixin
from .domain.project import ProjectEndpointMixin, ProjectTokenCollection
from .domain.round import RoundEndpointMixin
from .domain.score import ScoreEndpointMixin
from .domain.user import UserCollection, UserEndpointMixin
from .errors import ApiException, convert_error


class EndpointClient(
    requests.Session,
    CheckEndpointMixin,
    CompetitionEndpointMixin,
    CrunchEndpointMixin,
    DataReleaseEndpointMixin,
    PhaseEndpointMixin,
    PredictionEndpointMixin,
    ProjectEndpointMixin,
    RoundEndpointMixin,
    ScoreEndpointMixin,
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
    def users(self):
        return UserCollection(client=self)

    @property
    def project_tokens(self):
        return ProjectTokenCollection(
            competition=None,
            client=self
        )

    def format_web_url(self, path: str):
        return urllib.parse.urljoin(
            self.web_base_url,
            path
        )

    def from_env():
        store.load_from_env()

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

    def from_project():
        store.load_from_env()

        project_info = utils.read_project_info()
        push_token = utils.read_token()

        client = Client(
            store.api_base_url,
            store.web_base_url,
            PushTokenAuth(push_token)
        )

        return client, project_info
