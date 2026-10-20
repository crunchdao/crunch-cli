import os
import urllib.parse
from typing import Any, Callable, Iterable, Iterator, Literal, Sequence, cast, overload

import requests
from tqdm.auto import tqdm

import crunch.store as store
from crunch.api._auth import Auth
from crunch.api._errors import ApiException, convert_error
from crunch.api._pagination import PageRequest
from crunch.api._types import JsonValue
from crunch.constants import API_KEY_ENV_VAR
from crunch.utils import build_user_agent


class EndpointClient(requests.Session):

    def __init__(
        self,
        base_url: str,
        auth: Auth | None,
        show_progress: bool,
    ):
        super().__init__()

        self.base_url = base_url
        self.show_progress = show_progress
        self.page_size = 100

        self.headers.update({
            "User-Agent": build_user_agent(),
        })

        if auth is not None:
            self.headers["Authorization"] = auth.header_value

    def request(self, method: str, url: str, *args: Any, **kwargs: Any):
        headers: dict[str, str] = kwargs.pop("headers", None) or {}
        data: Any = kwargs.pop("data", None)
        files: Any = kwargs.pop("files", None)

        progress: tqdm | None = None

        if files is not None:
            import requests_toolbelt

            fields = [
                *files
            ]

            if isinstance(data, dict):
                fields.extend((
                    (key, str(value))
                    for key, value in cast(dict[str, Any], data).items()
                    if value is not None
                ))
            elif isinstance(data, (list, tuple)):
                # TODO Filter `None`s?
                fields.extend(cast(Iterable[tuple[str, Any]], data))
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
                data=data,
                files=files,
                **kwargs,
            )
        finally:
            if progress is not None:
                progress.close()

    def _raise_for_status(
        self,
        response: requests.Response,
        *,
        expect_json: bool = True,
    ):
        content_type = response.headers.get("Content-Type", "")
        is_json = content_type.startswith("application/json")

        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            if expect_json and not is_json:
                raise ApiException(f"received {response.status_code}: {response.text or '<no content>'}") from error

            content = error.response.json()
            converted = convert_error(content)

            raise converted

        return (
            content_type,
            is_json,
        )

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[True],
        binary: Literal[False] = False,
    ) -> JsonValue:
        ...

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[False] = False,
        *,
        binary: Literal[True],
    ) -> bytes:
        ...

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[False] = False,
        binary: Literal[False] = False,
    ) -> str:
        ...

    def _result(
        self,
        response: requests.Response,
        json: bool = False,
        binary: bool = False,
    ) -> JsonValue | bytes | str:
        assert not (json and binary)

        (
            content_type,
            is_json,
        ) = self._raise_for_status(response, expect_json=json)

        if json:
            if not is_json:
                raise ValueError(f"server did not return json: `{content_type}`: `{response.text}`")

            try:
                return cast(JsonValue, response.json())
            except requests.exceptions.JSONDecodeError as json_error:
                raise ValueError(f"could not parse json: `{response.text}`") from json_error

        if binary:
            return response.content

        return response.text

    def get_json(
        self,
        segments: Sequence[str | int],
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return self._result(self.get(_format_path(segments), params=params), json=True)

    def post_json(
        self,
        segments: Sequence[str | int],
        *,
        json: Any = None,
    ) -> Any:
        return self._result(self.post(_format_path(segments), json=json), json=True)

    def put_json(
        self,
        segments: Sequence[str | int],
        *,
        json: Any = None,
    ) -> Any:
        return self._result(self.put(_format_path(segments), json=json), json=True)

    def delete_json(
        self,
        segments: Sequence[str | int],
        *,
        json: Any = None,
    ) -> Any:
        return self._result(self.delete(_format_path(segments), json=json), json=True)

    def call(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        segments: Sequence[str | int],
        *,
        json: Any = None,
    ) -> None:
        """For the endpoints that answer without a body."""

        self._result(self.request(method, _format_path(segments), json=json))

    def paginate_json(
        self,
        segments: Sequence[str | int],
        *,
        params: dict[str, Any] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Any]:
        path = _format_path(segments)
        filters: dict[str, Any] = dict(params) if params is not None else {}

        return self._paginated(
            lambda page_request: self.get(
                path,
                params={
                    **filters,
                    "page": page_request.number,
                    "size": page_request.size,
                },
            ),
            page_size=page_size,
        )

    def _paginated(
        self,
        requester: Callable[[PageRequest], requests.Response],
        page_size: int | None = None
    ) -> Iterator[JsonValue]:
        if not page_size:
            page_size = self.page_size

        page_request = PageRequest(0, page_size)
        while True:
            response = requester(page_request)
            self._raise_for_status(response)

            try:
                json: dict[str, Any] = response.json()
            except requests.exceptions.JSONDecodeError as json_error:
                raise ValueError(f"could not parse json: `{response.text}`") from json_error

            content: list[JsonValue] = json["content"]
            for item in content:
                yield item

            if len(content) != json["pageSize"]:
                break

            page_request = page_request.next()


def _format_path(segments: Sequence[str | int]) -> str:
    """`["v1", "competitions", 3]` -> `/v1/competitions/3`, each segment is quoted (aliases like `@me` are kept)."""

    return "/" + "/".join(
        urllib.parse.quote(str(segment), safe="@")
        for segment in segments
    )


class Client:

    def __init__(
        self,
        api_base_url: str,
        auth: Auth | None,
        *,
        show_progress: bool = True,
    ):
        self.api = EndpointClient(api_base_url, auth, show_progress)

    @property
    def competitions(self):
        from crunch.api._domain.competition import CompetitionCollection
        return CompetitionCollection(self)

    @property
    def libraries(self):
        from crunch.api._domain.library import LibraryCollection
        return LibraryCollection(self)

    @property
    def uploads(self):
        from crunch.api._domain.upload import UploadCollection
        return UploadCollection(self)

    @property
    def users(self):
        from crunch.api._domain.user import UserCollection
        return UserCollection(self)

    @property
    def project_tokens(self):
        from crunch.api._domain.project import ProjectTokenCollection
        return ProjectTokenCollection(self)

    def get_runner_run(self, run_id: int):
        from crunch.api._domain.runner import RunnerRun
        return RunnerRun(run_id, self)

    @staticmethod
    def from_env(
        auth: Auth | None = None,
        *,
        show_progress: bool = True,
    ):
        if auth is None:
            api_key = os.getenv(API_KEY_ENV_VAR)
            if api_key:
                auth = Auth.api_key(api_key)

        return Client(
            store.api_base_url,
            auth,
            show_progress=show_progress,
        )
