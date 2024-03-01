import typing

from ..resource import Collection, Model
from .project import Project


class Submission(Model):

    resource_identifier_attribute = "number"

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
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def number(self) -> int:
        return self._attrs["number"]


class SubmissionCollection(Collection):

    model = Submission

    def __init__(
        self,
        project: Project,
        client=None
    ):
        super().__init__(client)

        self.project = project

    def __iter__(self) -> typing.Iterator[Submission]:
        return super().__iter__()

    def get(
        self,
        number: int
    ) -> Submission:
        return self.prepare_model(
            self._client.api.get_submission(
                self.project.competition.id,
                self.project.user_id,
                number
            )
        )

    def list(
        self
    ) -> typing.List[Submission]:
        return self.prepare_models(
            self._client.api.list_submissions(
                self.project.competition.id,
                self.project.user_id,
            )
        )
    
    def create(
        self,
        message: str,
        main_file_path: str,
        model_directory_path: str,
        notebook: bool,
        preferred_packages_version: typing.Dict[str, str],
        files: typing.List[typing.Tuple],
    ):
        return self.prepare_model(
            self._client.api.create_submission(
                self.project.competition.id,
                self.project.user_id,
                message,
                main_file_path,
                model_directory_path,
                notebook,
                preferred_packages_version,
                files,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.project
        )


class SubmissionEndpointMixin:

    def list_submissions(
        self,
        competition_identifier,
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/submissions"
            ),
            json=True
        )

    def get_submission(
        self,
        competition_identifier,
        user_identifier,
        submission_number
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/submissions/{submission_number}"
            ),
            json=True
        )

    def create_submission(
        self,
        competition_identifier,
        user_identifier,
        message,
        main_file_path,
        model_directory_path,
        notebook,
        preferred_packages_version,
        files,
    ):
        return self._result(
            self.post(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/submissions",
                data={
                    "message": message,
                    "mainFilePath": main_file_path,
                    "modelDirectoryPath": model_directory_path,
                    "notebook": notebook,
                    **{
                        f"preferredPackagesVersion[{key}]": value
                        for key, value in preferred_packages_version.items()
                    }
                },
                files=tuple(files)
            ),
            json=True
        )
