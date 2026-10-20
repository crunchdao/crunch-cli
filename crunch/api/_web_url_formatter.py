import urllib.parse

import crunch.store as store


class WebUrlFormatter:

    def __init__(self, base_url: str):
        self.base_url = base_url

    def home(self) -> str:
        return self._format()

    def account(self) -> str:
        return self._format("account")

    def competition(self, competition_name: str) -> str:
        return self._format("competitions", competition_name)

    def submit(self, competition_name: str) -> str:
        return self._format("competitions", competition_name, "submit")

    def submit_notebook(
        self,
        competition_name: str,
        *,
        project_name: str | None = None,
        message: str | None = None,
    ) -> str:
        return self._format(
            "competitions", competition_name, "submit", "notebook",
            query={
                "projectName": project_name,
                "message": message,
            },
        )

    def whitelisted_libraries(
        self,
        competition_name: str,
        *,
        request_name: str | None = None,
        request_alias: str | None = None,
        request_language: str | None = None,
    ) -> str:
        return self._format(
            "competitions", competition_name, "resources", "whitelisted-libraries",
            query={
                "requestName": request_name,
                "requestAlias": request_alias,
                "requestLanguage": request_language,
            },
        )

    def submission(
        self,
        competition_name: str,
        *,
        user_id: int,
        project_name: str,
        submission_number: int,
    ) -> str:
        return self._format("competitions", competition_name, "projects", user_id, project_name, "submissions", submission_number)

    def create_run(
        self,
        competition_name: str,
        *,
        user_login: str,
        project_name: str,
        submission_number: int,
    ) -> str:
        return self._format(
            "competitions", competition_name, "models", user_login, project_name, "runs", "create",
            query={
                "submissionNumber": submission_number,
            },
        )

    def deploy_submission(
        self,
        competition_name: str,
        *,
        user_login: str,
        project_name: str,
        submission_number: int,
        submission_id: int,
    ) -> str:
        return self._format(
            "competitions", competition_name, "models", user_login, project_name, "submissions", submission_number,
            query={
                f"openDeploy-{submission_id}": "true",
            },
        )

    def _format(
        self,
        *segments: str | int,
        query: dict[str, str | int | None] | None = None,
    ) -> str:
        path = "/".join(
            urllib.parse.quote(str(segment), safe="")
            for segment in segments
        )

        url = urllib.parse.urljoin(self.base_url, f"/{path}")

        parameters = {
            key: value
            for key, value in (query or {}).items()
            if value is not None
        }

        if parameters:
            url += "?" + urllib.parse.urlencode(parameters)

        return url

    @staticmethod
    def from_env() -> "WebUrlFormatter":
        return WebUrlFormatter(store.web_base_url)
