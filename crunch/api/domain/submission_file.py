import typing

from ..resource import Collection, Model
from .submission import Submission


class SubmissionFile(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        submission: Submission,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._submission = submission

    @property
    def submission(self):
        return self._submission

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def size(self) -> int:
        return self._attrs["size"]

    @property
    def hash(self) -> str:
        return self._attrs["hash"]

    @property
    def mime_type(self) -> str:
        return self._attrs["mimeType"]

    @property
    def found_hardcoded_string(self) -> bool:
        return self._attrs["foundHardcodedString"]


class SubmissionFileCollection(Collection):

    model = SubmissionFile

    def __init__(
        self,
        submission: Submission,
        client=None
    ):
        super().__init__(client)

        self.submission = submission

    def __iter__(self) -> typing.Iterator[SubmissionFile]:
        return super().__iter__()

    def list(
        self
    ) -> typing.List[SubmissionFile]:
        from_attrs = self.submission._attrs.get("files")
        if from_attrs:
            return self.prepare_models(from_attrs)

        return self.prepare_models(
            self._client.api.list_submission_files(
                self.submission.project.competition.id,
                self.submission.project.user_id,
                self.submission.project.name,
                self.submission.number,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.submission
        )


class SubmissionFileEndpointMixin:

    def list_submission_files(
        self,
        competition_identifier,
        user_identifier,
        project_identifier,
        submission_number,
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/{submission_number}/files"
            ),
            json=True
        )
