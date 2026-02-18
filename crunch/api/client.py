import os
import urllib.parse
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional, Tuple, cast

import requests
from tqdm.auto import tqdm
from urllib3.exceptions import RequestError

import crunch.store as store
from crunch.api.auth import ApiKeyAuth, Auth, NoneAuth, PushTokenAuth
from crunch.api.domain.check import CheckEndpointMixin
from crunch.api.domain.competition import CompetitionCollection, CompetitionEndpointMixin
from crunch.api.domain.crunch import CrunchEndpointMixin
from crunch.api.domain.data_release import DataReleaseEndpointMixin
from crunch.api.domain.leaderboard import LeaderboardEndpointMixin
from crunch.api.domain.library import LibraryCollection, LibraryEndpointMixin
from crunch.api.domain.metric import MetricEndpointMixin
from crunch.api.domain.phase import PhaseEndpointMixin
from crunch.api.domain.prediction import PredictionEndpointMixin
from crunch.api.domain.project import Project, ProjectEndpointMixin, ProjectTokenCollection
from crunch.api.domain.quickstarter import QuickstarterEndpointMixin
from crunch.api.domain.round import RoundEndpointMixin
from crunch.api.domain.run import RunEndpointMixin
from crunch.api.domain.runner import RunnerRun, RunnerRunEndpointMixin
from crunch.api.domain.score import ScoreEndpointMixin
from crunch.api.domain.submission import SubmissionEndpointMixin
from crunch.api.domain.submission_file import SubmissionFileEndpointMixin
from crunch.api.domain.target import TargetEndpointMixin
from crunch.api.domain.upload import UploadCollection, UploadEndpointMixin
from crunch.api.domain.user import UserCollection, UserEndpointMixin
from crunch.api.errors import convert_error
from crunch.api.pagination import PageRequest
from crunch.constants import API_KEY_ENV_VAR

if TYPE_CHECKING:
    from crunch.utils import ProjectInfo


class EndpointClient(
    requests.Session,
    CheckEndpointMixin,
    CompetitionEndpointMixin,
    CrunchEndpointMixin,
    DataReleaseEndpointMixin,
    LeaderboardEndpointMixin,
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

    def request(self, method: str, url: str, *args: Any, **kwargs: Any):
        headers = kwargs.pop("headers", {})
        params = kwargs.pop("params", {})
        data: Any = kwargs.pop("data", None)
        files: Any = kwargs.pop("files", None)

        self.auth_.apply(headers, params, data)

        progress: Optional[tqdm] = None

        if files is not None:
            import requests_toolbelt

            fields = [
                *files
            ]

            if isinstance(data, dict):
                fields.extend((
                    (key, str(value))
                    for key, value in cast(Dict[str, Any], data).items()
                    if value is not None
                ))
            elif isinstance(data, (list, tuple)):
                # TODO Filter `None`s?
                fields.extend(cast(Iterable[Tuple[str, Any]], data))
            elif data is not None:
                raise ValueError(f"unsupported data: {data}")

            encoder = requests_toolbelt.MultipartEncoder(fields)
            headers["Content-Type"] = encoder.content_type
            files = None

            if not self.show_progress:
                data = encoder
            else:
                progress = tqdm(
                    desc="upload",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    total=encoder.len,  # type: ignore
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

                if isinstance(arg, RequestError):
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
        json: bool = False,
        binary: bool = False,
    ):
        assert not (json and binary)
        self._raise_for_status(response)

        content_type = response.headers.get("content-type")

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

    def _paginated(
        self,
        requester: Callable[[PageRequest], requests.Response],
        page_size: Optional[int] = None
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
        project_info: Optional["ProjectInfo"] = None,
        *,
        show_progress: bool = True,
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
        auth: Optional[Auth] = None,
        *,
        show_progress: bool = True,
    ):
        store.load_from_env()

        if auth is None:
            api_key = os.getenv(API_KEY_ENV_VAR)
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
        show_progress: bool = True,
    ) -> Tuple["Client", Project]:
        from crunch.utils import read_project_info, read_token

        store.load_from_env()

        project_info = read_project_info()
        push_token = read_token()

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
