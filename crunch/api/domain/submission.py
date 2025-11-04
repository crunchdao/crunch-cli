from enum import Enum
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

from crunch.api.auth import PushTokenAuth
from crunch.api.resource import Collection, Model

if TYPE_CHECKING:
    from crunch.api.client import Client
    from crunch.api.domain.project import Project
    from crunch.api.types import Attrs


class SubmissionType(Enum):

    CODE = "CODE"
    NOTEBOOK = "NOTEBOOK"
    PREDICTION = "PREDICTION"

    def __repr__(self):
        return self.name


class Submission(Model):

    resource_identifier_attribute = "number"

    def __init__(
        self,
        project: "Project",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["SubmissionCollection"] = None,
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

    @property
    def files(self):
        from .submission_file import SubmissionFileCollection

        return SubmissionFileCollection(self, self._client)


class SubmissionCollection(Collection):

    model = Submission

    def __init__(
        self,
        project: "Project",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.project = project

    def __iter__(self) -> Iterator[Submission]:
        return super().__iter__()

    def get(
        self,
        number: int,
    ) -> Submission:
        return self.prepare_model(
            self._client.api.get_submission(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                number
            )
        )

    def list(
        self
    ) -> List[Submission]:
        return self.prepare_models(
            self._client.api.list_submissions(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
            )
        )

    def create(
        self,
        *,
        message: str,
        main_file_path: str,
        model_directory_path: str,
        type: SubmissionType,
        code_files: Dict[str, str],
        model_files: Dict[str, str],
    ) -> Submission:
        return self.prepare_model(
            self._client.api.create_submission(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                message,
                main_file_path,
                model_directory_path,
                type.name,
                code_files,
                model_files,
            )
        )

    def get_next_encryption_id(self) -> str:
        return self._client.api.get_submission_next_encryption_id(
            self.project.competition.id,
            self.project.user_id,
            self.project.name,
        )

    def prepare_model(self, attrs: "Attrs"):
        return super().prepare_model(
            attrs,
            self.project,
        )


class SubmissionEndpointMixin:

    def list_submissions(
        self,
        competition_identifier,
        user_identifier,
        project_identifier
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions"
            ),
            json=True
        )

    def get_submission(
        self,
        competition_identifier,
        user_identifier,
        project_identifier,
        submission_number
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/{submission_number}"
            ),
            json=True
        )

    def create_submission(
        self,
        competition_identifier,
        user_identifier,
        project_identifier,
        message,
        main_file_path,
        model_directory_path,
        type,
        code_files,
        model_files,
    ):
        return self._result(
            self.post(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions",
                json={
                    "message": message,
                    "mainFilePath": main_file_path,
                    "modelDirectoryPath": model_directory_path,
                    "type": type,
                    "codeFiles": code_files,
                    "modelFiles": model_files,
                    # TODO Use a better way to pass the push token
                    "pushToken": self.auth_._token if isinstance(self.auth_, PushTokenAuth) else None,
                },
            ),
            json=True,
        )

    def get_submission_next_encryption_id(
        self,
        competition_identifier,
        user_identifier,
        project_identifier
    ):
        return self._result(
            self.get(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/next-encryption-id",
                params={
                    "pushToken": self.auth_._token if isinstance(self.auth_, PushTokenAuth) else None,
                },
            ),
        )
