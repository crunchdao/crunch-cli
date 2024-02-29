import urllib.parse

import requests

from ..auth import Auth
from ..errors import ApiException
from .check import CheckEndpointMixin
from .competition import CompetitionEndpointMixin
from .crunch import CrunchEndpointMixin
from .data_release import DataReleaseEndpointMixin
from .phase import PhaseEndpointMixin
from .prediction import PredictionEndpointMixin
from .project import ProjectEndpointMixin
from .round import RoundEndpointMixin
from .score import ScoreEndpointMixin


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

            code = content.pop("code", "")
            message = content.pop("message", "")

            raise ApiException(
                f"{code}: {message}"
            )

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
