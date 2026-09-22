from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from crunch.api._auth import PushTokenAuth
from crunch.api._resource import Collection, EndpointMixin
from crunch.api._resource import Model as BaseModel

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.model import Model
    from crunch.api._domain.project import Project
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class SubmissionType(Enum):

    CODE = "CODE"
    NOTEBOOK = "NOTEBOOK"
    PREDICTION = "PREDICTION"

    def __repr__(self):
        return self.name


class Submission(BaseModel[int]):

    def __init__(
        self,
        project: "Project",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["SubmissionCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._project = project

    @property
    def resource_identifier(self) -> int:
        return self.number

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
    def message(self) -> str:
        return self._attrs["message"]

    @property
    def main_file_path(self) -> str:
        return self._attrs["mainFilePath"]

    @property
    def model_directory_path(self) -> str:
        return self._attrs["modelDirectoryPath"]

    @property
    def total_size(self) -> int:
        return self._attrs["totalSize"]

    @property
    def model(self) -> Optional["Model"]:
        from crunch.api._domain.model import Model

        model_attrs = self._attrs.get("model")
        if model_attrs is None:
            return None

        return Model(self.project, model_attrs, self._client)

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])

    @property
    def files(self):
        from crunch.api._domain.submission_file import SubmissionFileCollection

        return SubmissionFileCollection(self, self._client)

    @property
    def runtime_options(self):
        from crunch.api._domain.runtime import RuntimeOptionCollection

        return RuntimeOptionCollection(self, self._client)


class SubmissionCollection(Collection[Submission]):

    model = Submission

    def __init__(
        self,
        project: "Project",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.project = project

    def get(
        self,
        number: int,
    ) -> Submission:
        return self.prepare_model(
            self._checked_client.api.get_submission(
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
            self._checked_client.api.list_submissions(
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
            self._checked_client.api.create_submission(
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
        return self._checked_client.api.get_submission_next_encryption_id(
            self.project.competition.id,
            self.project.user_id,
            self.project.name,
        )

    def prepare_model(self, attrs: Union["JsonValue", Submission], *args: Any) -> Submission:
        return super().prepare_model(
            attrs,
            self.project,
            *args
        )


class SubmissionEndpointMixin(EndpointMixin):

    def list_submissions(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions"
            ),
            json=True
        )

    def get_submission(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        submission_number: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/{submission_number}"
            ),
            json=True
        )

    def create_submission(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        message: str,
        main_file_path: str,
        model_directory_path: str,
        type: str,
        code_files: Dict[str, str],
        model_files: Dict[str, str],
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
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/next-encryption-id",
                params={
                    "pushToken": self.auth_._token if isinstance(self.auth_, PushTokenAuth) else None,
                },
            ),
        )
