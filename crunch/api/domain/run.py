import typing

from ..resource import Collection, Model
from .project import Project


class Run(Model):

    def __init__(
        self,
        project: Project,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._project = project

    @property
    def project(self):
        return self._project

    @property
    def success(self) -> bool:
        return self._attrs["success"]

    @property
    def error(self) -> typing.Optional[str]:
        return self._attrs["error"]


class RunCollection(Collection):

    model = Run

    def __init__(
        self,
        project: Project,
        client=None
    ):
        super().__init__(client)

        self.project = project

    def __iter__(self) -> typing.Iterator[Run]:
        return super().__iter__()

    def get(
        self,
        id: int
    ) -> Run:
        return self.prepare_model(
            self._client.api.get_run(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                id
            )
        )

    def list(
        self,
        managed: typing.Optional[bool] = None,
        submission: typing.Optional["Submission"] = None,
        submission_number: typing.Optional[int] = None
    ) -> typing.List[Run]:
        assert not ((submission is not None) and (submission_number is not None))

        return self.prepare_models(
            self._client.api.list_runs(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                managed,
                submission.number if submission is not None else submission_number,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.project
        )


class RunEndpointMixin:

    def list_runs(
        self,
        competition_identifier,
        user_identifier,
        project_identifier,
        managed,
        submission_number
    ):
        params = {}

        if managed is not None:
            params["managed"] = managed

        if submission_number is not None:
            params["submissionNumber"] = submission_number

        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs",
                params=params
            ),
            json=True
        )

    def get_run(
        self,
        competition_identifier,
        user_identifier,
        project_identifier,
        run_id
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs/{run_id}"
            ),
            json=True
        )
