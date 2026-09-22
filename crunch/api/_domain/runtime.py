from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

from dataclasses_json import LetterCase, Undefined, dataclass_json

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
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

    count: Optional[int]
    model: Optional[str]
    driver: Optional[str]


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinitionSpecification:

    memory: RuntimeDefinitionSpecificationSize
    cpu: RuntimeDefinitionSpecificationCpu
    gpu: RuntimeDefinitionSpecificationGpu


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RuntimeDefinition:

    id: int
    name: str
    type: RuntimeType
    display_name: str
    description: str
    powered_by: RuntimeDefinitionPoweredBy
    specification: RuntimeDefinitionSpecification


class RuntimeOptionStatus(Enum):
    AVAILABLE = "AVAILABLE"
    REQUESTABLE = "REQUESTABLE"
    REQUESTED = "REQUESTED"
    BLOCKED = "BLOCKED"


class RuntimeOption(Model[int]):

    def __init__(
        self,
        submission: "Submission",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["RuntimeOptionCollection"] = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._submission = submission

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
        return RuntimeDefinition.from_dict(self._attrs["definition"])  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]


class RuntimeOptionCollection(Collection[RuntimeOption]):

    model = RuntimeOption

    def __init__(
        self,
        submission: "Submission",
        client: Optional["Client"] = None
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

    def prepare_model(self, attrs: Union["JsonValue", RuntimeOption], *args: Any) -> RuntimeOption:
        return super().prepare_model(
            attrs,
            self.submission,
            *args
        )


class RuntimeOptionEndpointMixin(EndpointMixin):

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
