from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterator

from dataclasses_json import LetterCase, Undefined, dataclass_json

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.submission import Submission
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class RuntimeType(Enum):
    CPU = "CPU"
    GPU = "GPU"
    COPY = "COPY"  # internal mechanism


class RuntimeDefinitionStatus(Enum):
    ACTIVE = "ACTIVE"
    RESTRICTED = "RESTRICTED"
    DISABLED = "DISABLED"


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionPoweredBy:

    text: str
    url: str


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionSpecificationSize:

    size: int


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionSpecificationCpu:

    core_count: int


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionSpecificationGpu:

    count: int | None
    model: str | None
    driver: str | None


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionSpecification:

    memory: RuntimeDefinitionSpecificationSize
    cpu: RuntimeDefinitionSpecificationCpu
    gpu: RuntimeDefinitionSpecificationGpu


class RuntimeOptionStatus(Enum):
    AVAILABLE = "AVAILABLE"
    REQUESTABLE = "REQUESTABLE"
    REQUESTED = "REQUESTED"
    BLOCKED = "BLOCKED"


class RuntimeDefinition(Model[str]):

    def __init__(
        self,
        submission: "Submission",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "RuntimeOptionCollection | None" = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._submission = submission

    @property
    def id(self) -> str:
        return self._attrs["name"]

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def type(self):
        return RuntimeType[self._attrs["type"]]

    @property
    def display_name(self) -> str:
        return self._attrs["displayName"]

    @property
    def description(self) -> str:
        return self._attrs["description"]

    @property
    def powered_by(self) -> RuntimeDefinitionPoweredBy:
        return RuntimeDefinitionPoweredBy.from_dict(self._attrs["poweredBy"])  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]

    @property
    def specification(self) -> RuntimeDefinitionSpecification:
        return RuntimeDefinitionSpecification.from_dict(self._attrs["specification"])  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]


class RuntimeDefinitionCollection(Collection[RuntimeDefinition]):

    model = RuntimeDefinition

    def __init__(
        self,
        competition: "Competition",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.competition = competition

    def list(
        self
    ) -> Iterator[RuntimeDefinition]:
        return self.prepare_models(
            self._checked_client.api.list_runtime_definitions(
                self.competition.id,
            )
        )

    def prepare_model(self, attrs: "JsonValue | RuntimeDefinition", *args: Any) -> RuntimeDefinition:
        return super().prepare_model(
            attrs,
            self.competition,
            *args
        )


class RuntimeOption(Model[int]):

    def __init__(
        self,
        submission: "Submission",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "RuntimeOptionCollection | None" = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._submission = submission

    @property
    def id(self) -> int:
        return self._attrs["definition"]["id"]

    @property
    def submission(self):
        return self._submission

    @property
    def quota(self):
        return timedelta(seconds=self._attrs["quota"])

    @property
    def status(self):
        return RuntimeOptionStatus[self._attrs["status"]]

    @property
    def definition(self) -> RuntimeDefinition:
        return RuntimeDefinition.from_dict(self._attrs["definition"], None)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]


class RuntimeOptionCollection(Collection[RuntimeOption]):

    model = RuntimeOption

    def __init__(
        self,
        submission: "Submission",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.submission = submission

    def list(
        self
    ) -> Iterator[RuntimeOption]:
        return self.prepare_models(
            self._checked_client.api.list_runtime_options(
                self.submission.project.competition.id,
                self.submission.project.user_id,
                self.submission.project.name,
                self.submission.number,
            )
        )

    def prepare_model(self, attrs: "JsonValue | RuntimeOption", *args: Any) -> RuntimeOption:
        return super().prepare_model(
            attrs,
            self.submission,
            *args
        )


class RuntimeAccessRequestVerdict(Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class RuntimeAccessRequest(Model[int]):

    def __init__(
        self,
        submission: "Submission",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "RuntimeAccessRequestCollection | None" = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._submission = submission

    @property
    def submission(self):
        return self._submission

    @property
    def definition(self) -> RuntimeDefinition:
        return RuntimeDefinition.from_dict(self._attrs["definition"], None)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]

    @property
    def justification(self) -> str | None:
        return self._attrs.get("justification")

    @property
    def verdict(self):
        return RuntimeAccessRequestVerdict[self._attrs["verdict"]]

    @property
    def verdict_reason(self) -> str | None:
        return self._attrs.get("verdictReason")

    @property
    def verdict_at(self) -> datetime | None:
        verdict_at = self._attrs.get("verdictAt")
        return datetime.fromisoformat(verdict_at) if verdict_at else None

    @property
    def further_request_blocked(self) -> bool:
        return bool(self._attrs.get("furtherRequestBlocked"))

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])


class RuntimeAccessRequestCollection(Collection[RuntimeAccessRequest]):

    model = RuntimeAccessRequest

    def __init__(
        self,
        submission: "Submission",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.submission = submission

    def create(
        self,
        *,
        definition_name: str,
        justification: str | None = None,
    ) -> RuntimeAccessRequest:
        project = self.submission.project

        return self.prepare_model(
            self._checked_client.api.create_runtime_access_request(
                competition_id=project.competition.id,
                project_id=project.id,
                submission_number=self.submission.number,
                definition_name=definition_name,
                justification=justification,
            )
        )

    def prepare_model(self, attrs: "JsonValue | RuntimeAccessRequest", *args: Any) -> RuntimeAccessRequest:
        return super().prepare_model(
            attrs,
            self.submission,
            *args
        )


class RuntimeEndpointMixin(EndpointMixin):

    def list_runtime_definitions(
        self,
        competition_identifier: "CompetitionIdentifierType",
    ):
        return self._paginated(
            lambda page_request: self.get(
                f"/v1/competitions/{competition_identifier}/runtime-definitions",
                params={
                    "page": page_request.number,
                    "size": page_request.size,
                },
            ),
            page_size=1000,
        )

    def list_runtime_options(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        submission_number: int,
    ):
        return self._paginated(
            lambda page_request: self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/submissions/{submission_number}/runtime-options",
                params={
                    "page": page_request.number,
                    "size": page_request.size,
                },
            ),
            page_size=1000,
        )

    def create_runtime_access_request(
        self,
        competition_id: int,
        project_id: int,
        submission_number: int,
        definition_name: str,
        justification: str | None,
    ):
        return self._result(
            self.post(
                "/v1/runtime-access-requests",
                json={
                    "competitionId": competition_id,
                    "projectId": project_id,
                    "submissionNumber": submission_number,
                    "definitionName": definition_name,
                    "justification": justification,
                },
            ),
            json=True
        )
