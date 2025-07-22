import json
import os
import typing
import urllib.parse

import requests
import tqdm
import urllib3

if typing.TYPE_CHECKING:
    from ..external import sseclient

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
from .domain.submission_file import SubmissionFileEndpointMixin
from .domain.target import TargetEndpointMixin
from .domain.upload import UploadCollection, UploadEndpointMixin
from .domain.user import UserCollection, UserEndpointMixin
from .errors import convert_error
from .pagination import PageRequest


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
    SubmissionFileEndpointMixin,
    TargetEndpointMixin,
    UploadEndpointMixin,
    UserEndpointMixin,
):

    def __init__(
        self,
        base_url: str,
        auth: Auth,
        show_progress: bool,
    ):
        super().__init__()

        self.base_url = base_url
        self.auth_ = auth
        self.show_progress = show_progress
        self.page_size = 100

    def request(self, method, url, *args, **kwargs):
        headers = kwargs.pop("headers", {})
        params = kwargs.pop("params", {})
        data: dict = kwargs.pop("data", None)
        files: tuple = kwargs.pop("files", None)

        self.auth_.apply(headers, params, data)

        progress: tqdm.tqdm = None

        if files is not None:
            import requests_toolbelt

            fields = [
                *files
            ]

            if isinstance(data, dict):
                fields.extend((
                    (key, str(value))
                    for key, value in data.items()
                    if value is not None
                ))
            elif isinstance(data, (list, tuple)):
                # TODO Filter `None`s?
                fields.extend(data)
            elif data is not None:
                raise ValueError(f"unsupported data: {data}")

            encoder = requests_toolbelt.MultipartEncoder(fields)
            headers["Content-Type"] = encoder.content_type
            files = None

            if not self.show_progress:
                data = encoder
            else:
                progress = tqdm.tqdm(
                    desc="upload",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    total=encoder.len,
                )

                def callback(monitor: requests_toolbelt.MultipartEncoderMonitor):
                    progress.update(monitor.bytes_read - progress.n)
                    if progress.total == progress.n:
                        progress.refresh(progress.lock_args)

                data = requests_toolbelt.MultipartEncoderMonitor(encoder, callback)

        try:
            return super().request(
                method,
                urllib.parse.urljoin(self.base_url, url),
                *args,
                headers=headers,
                params=params,
                data=data,
                files=files,
                **kwargs,
            )
        except requests.exceptions.RequestException as error:
            self._strip_secrets(error)
            raise error
        finally:
            if progress is not None:
                progress.close()

    def _raise_for_status(
        self,
        response: requests.Response,
    ):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            self._strip_secrets(error)

            content = error.response.json()
            converted = convert_error(content)

            raise converted

    def _strip_secrets(self, error: BaseException):
        if not self.auth_:
            return None

        args = list(error.args)

        for index, arg in enumerate(args):
            if isinstance(arg, BaseException):
                self._strip_secrets(arg)

                if isinstance(arg, urllib3.exceptions.RequestError):
                    arg.url = self.auth_.strip(arg.url) or arg.url

                continue

            if not isinstance(arg, str):
                continue

            new_arg = self.auth_.strip(arg)
            if new_arg is not None:
                args[index] = new_arg

        error.args = tuple(args)

        cause = error.__cause__
        if cause is not None:
            self._strip_secrets(cause)

    def _result(
        self,
        response: requests.Response,
        json=False,
        binary=False,
        sse_handler=None,
    ):
        assert not (json and binary)
        self._raise_for_status(response)

        content_type = response.headers.get("content-type")

        if sse_handler and content_type == "text/event-stream":
            return self._result_sse(response, sse_handler, json)

        if json:
            if content_type != "application/json":
                raise ValueError(f"server did not return json: `{content_type}`: `{response.text}`")

            try:
                return response.json()
            except requests.exceptions.JSONDecodeError as json_error:
                raise ValueError(f"could not parse json: `{response.text}`") from json_error

        if binary:
            return response.content

        return response.text

    def _result_sse(
        self,
        response: requests.Response,
        sse_handler: typing.Callable[["sseclient.Event"], None],
        as_json=False,
    ):
        from ..external import sseclient

        client = sseclient.SSEClient(response)
        for event in client.events():
            is_error = event.event.startswith("error:")
            is_result = event.event == "result"

            if as_json or is_error:
                try:
                    event.data = json.loads(event.data)
                except json.decoder.JSONDecodeError:
                    pass

            if is_error:
                converted = convert_error(event.data)
                raise converted

            if is_result:
                return event.data

            sse_handler(event)

    def _paginated(
        self,
        requester: typing.Callable[[PageRequest], requests.Response],
        page_size: typing.Optional[int] = None
    ):
        if not page_size:
            page_size = self.page_size

        page_request = PageRequest(0, page_size)
        while True:
            response = requester(page_request)
            self._raise_for_status(response)

            try:
                json = response.json()
            except requests.exceptions.JSONDecodeError as json_error:
                raise ValueError(f"could not parse json: `{response.text}`") from json_error

            content = json["content"]
            for item in content:
                yield item

            if len(content) != json["pageSize"]:
                break

            page_request = page_request.next()


class Client:

    def __init__(
        self,
        api_base_url: str,
        web_base_url: str,
        auth: Auth,
        project_info: typing.Optional[utils.ProjectInfo] = None,
        *,
        show_progress=True,
    ):
        self.api = EndpointClient(api_base_url, auth, show_progress)
        self.web_base_url = web_base_url
        self.project_info = project_info

    @property
    def competitions(self):
        return CompetitionCollection(client=self)

    @property
    def libraries(self):
        return LibraryCollection(client=self)

    @property
    def uploads(self):
        return UploadCollection(client=self)

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
        auth: typing.Optional[Auth] = None,
        *,
        show_progress=True,
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
            auth,
            show_progress=show_progress,
        )

    @staticmethod
    def from_project(
        *,
        show_progress=True,
    ) -> typing.Tuple["Client", Project]:
        store.load_from_env()

        project_info = utils.read_project_info()
        push_token = utils.read_token()

        client = Client(
            store.api_base_url,
            store.web_base_url,
            PushTokenAuth(push_token),
            project_info,
            show_progress=show_progress,
        )

        competition = client.competitions.get(project_info.competition_name)
        project = competition.projects.get_reference(None, (project_info.user_id, project_info.project_name))

        return client, project
