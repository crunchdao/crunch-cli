from typing import TYPE_CHECKING, Any, cast

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.submission import Submission
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class SubmissionFile(Model[int]):

    def __init__(
        self,
        submission: "Submission",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "SubmissionFileCollection | None" = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._submission = submission

    @property
    def resource_identifier(self) -> str:
        return self.name

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


class SubmissionFileCollection(Collection[SubmissionFile]):

    model = SubmissionFile

    def __init__(
        self,
        submission: "Submission",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.submission = submission

    def list(
        self
    ) -> list[SubmissionFile]:
        from_attrs = self.submission._attrs.get("files")  # pyright: ignore[reportPrivateUsage]
        if from_attrs:
            return self.prepare_models(cast("JsonValue", from_attrs))

        return self.prepare_models(
            self._checked_client.api.list_submission_files(
                self.submission.project.competition.id,
                self.submission.project.user_id,
                self.submission.project.name,
                self.submission.number,
            )
        )

    def prepare_model(self, attrs: "JsonValue | SubmissionFile", *args: Any) -> SubmissionFile:
        return super().prepare_model(
            attrs,
            self.submission,
            *args
        )


class SubmissionFileEndpointMixin(EndpointMixin):

    def list_submission_files(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        submission_number: int,
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/{submission_number}/files"
            ),
            json=True
        )
