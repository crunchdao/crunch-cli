from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._domain.model import Model

if TYPE_CHECKING:
    from crunch.api._domain.project import ProjectReference
    from crunch.api._identifiers import SubmissionIdentifierType


class SubmissionType(ApiEnum):
    CODE = "CODE"
    NOTEBOOK = "NOTEBOOK"
    PREDICTION = "PREDICTION"


@dataclass
class Submission(ApiModel):
    id: int
    number: int
    type: SubmissionType
    message: str
    main_file_path: str
    model_directory_path: str
    total_size: int
    created_at: datetime
    model: Model | None = None

    project = Parent["ProjectReference"]()

    @property
    def files(self) -> list["SubmissionFile"]:
        project = self.project

        files = SubmissionFile.from_dict_array(self.project.client.api.get_json([
            "v3", "competitions", project.competition.id, "projects", project.user_id, project.name, "submissions", self.number, "files"
        ]))

        for file in files:
            file.submission = self

        return files

    @property
    def runtime_options(self):
        from crunch.api._domain.runtime import RuntimeOptionCollection
        return RuntimeOptionCollection(self)

    @property
    def runtime_access_requests(self):
        from crunch.api._domain.runtime import RuntimeAccessRequestCollection
        return RuntimeAccessRequestCollection(self)


class SubmissionCollection(ApiCollection[Submission]):

    model = Submission

    def __init__(self, project: "ProjectReference"):
        super().__init__(project.client)

        self.project = project

    def _attach(self, submission: Submission) -> Submission:
        submission.project = self.project

        if submission.model is not None:
            submission.model.project = self.project

        return submission

    def get(self, /, identifier: "SubmissionIdentifierType") -> Submission:
        return self.from_dict(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "submissions", identifier
        ]))

    def list(self) -> list[Submission]:
        return self.from_dict_array(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "submissions"
        ]))

    def create(
        self,
        *,
        message: str,
        main_file_path: str,
        model_directory_path: str,
        type: SubmissionType,
        code_files: dict[str, str],
        model_files: dict[str, str],
        submitter_imprint: str,
    ) -> Submission:
        return self.from_dict(self.client.api.post_json(
            ["v4", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "submissions"],
            json={
                "message": message,
                "mainFilePath": main_file_path,
                "modelDirectoryPath": model_directory_path,
                "type": type.name,
                "codeFiles": code_files,
                "modelFiles": model_files,
                "submitterImprint": submitter_imprint,
            },
        ))


@dataclass
class SubmissionFile(ApiModel):
    name: str
    size: int
    mime_type: str

    submission = Parent[Submission]()
