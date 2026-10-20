import os

import crunch.store as store
from crunch.api._auth import Auth
from crunch.constants import API_KEY_ENV_VAR


class Client:

    def __init__(
        self,
        api_base_url: str,
        auth: Auth | None,
        *,
        show_progress: bool = True,
    ):
        from crunch.api._endpoint import EndpointClient

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
