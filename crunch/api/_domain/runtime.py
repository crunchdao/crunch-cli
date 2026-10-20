from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterator

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.submission import Submission


class RuntimeType(ApiEnum):
    CPU = "CPU"
    GPU = "GPU"
    COPY = "COPY"  # internal mechanism


class RuntimeDefinitionStatus(ApiEnum):
    ACTIVE = "ACTIVE"
    RESTRICTED = "RESTRICTED"
    DISABLED = "DISABLED"


@dataclass(frozen=True)
class RuntimeDefinitionPoweredBy(JsonDataClassMixin):
    text: str
    url: str


@dataclass(frozen=True)
class RuntimeDefinitionSpecificationSize(JsonDataClassMixin):
    size: int


@dataclass(frozen=True)
class RuntimeDefinitionSpecificationCpu(JsonDataClassMixin):
    core_count: int


@dataclass(frozen=True)
class RuntimeDefinitionSpecificationGpu(JsonDataClassMixin):
    count: int | None
    model: str | None
    driver: str | None


@dataclass(frozen=True)
class RuntimeDefinitionSpecification(JsonDataClassMixin):
    memory: RuntimeDefinitionSpecificationSize
    cpu: RuntimeDefinitionSpecificationCpu
    gpu: RuntimeDefinitionSpecificationGpu


class RuntimeOptionStatus(ApiEnum):
    AVAILABLE = "AVAILABLE"
    REQUESTABLE = "REQUESTABLE"
    REQUESTED = "REQUESTED"
    BLOCKED = "BLOCKED"


@dataclass
class RuntimeDefinition(ApiModel):
    id: int
    name: str
    type: RuntimeType
    status: RuntimeDefinitionStatus
    display_name: str
    description: str
    powered_by: RuntimeDefinitionPoweredBy
    specification: RuntimeDefinitionSpecification

    competition = Parent["Competition"]()


class RuntimeDefinitionCollection(ApiCollection[RuntimeDefinition]):

    model = RuntimeDefinition

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, definition: RuntimeDefinition) -> RuntimeDefinition:
        definition.competition = self.competition
        return definition

    def iter(self) -> Iterator[RuntimeDefinition]:
        return self.from_dict_iterator(self.client.api.paginate_json(
            ["v1", "competitions", self.competition.id, "runtime-definitions"],
            page_size=1000,
        ))


@dataclass
class RuntimeOption(ApiModel):
    definition: RuntimeDefinition
    quota: timedelta
    recommended: bool
    status: RuntimeOptionStatus

    submission = Parent["Submission"]()


class RuntimeOptionCollection(ApiCollection[RuntimeOption]):

    model = RuntimeOption

    def __init__(self, submission: "Submission"):
        super().__init__(submission.project.client)

        self.submission = submission

    def _attach(self, option: RuntimeOption) -> RuntimeOption:
        option.submission = self.submission
        option.definition.competition = self.submission.project.competition
        return option

    def iter(self) -> Iterator[RuntimeOption]:
        project = self.submission.project

        return self.from_dict_iterator(self.client.api.paginate_json(
            ["v3", "competitions", project.competition.id, "projects", project.user_id, project.name, "submissions", self.submission.number, "runtime-options"],
            page_size=1000,
        ))


class RuntimeAccessRequestVerdict(ApiEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class RuntimeAccessRequest(ApiModel):
    id: int
    definition: RuntimeDefinition
    verdict: RuntimeAccessRequestVerdict
    created_at: datetime
    justification: str | None = None
    verdict_reason: str | None = None
    verdict_at: datetime | None = None
    further_request_blocked: bool = False

    submission = Parent["Submission"]()


class RuntimeAccessRequestCollection(ApiCollection[RuntimeAccessRequest]):

    model = RuntimeAccessRequest

    def __init__(self, submission: "Submission"):
        super().__init__(submission.project.client)

        self.submission = submission

    def _attach(self, access_request: RuntimeAccessRequest) -> RuntimeAccessRequest:
        access_request.submission = self.submission
        access_request.definition.competition = self.submission.project.competition
        return access_request

    def create(
        self,
        *,
        definition_name: str,
        justification: str | None = None,
    ) -> RuntimeAccessRequest:
        from crunch.api._domain.project import Project

        project = self.submission.project

        if not isinstance(project, Project):
            project = project.reload()

        return self.from_dict(self.client.api.post_json(
            ["v1", "runtime-access-requests"],
            json={
                "competitionId": project.competition.id,
                "projectId": project.id,
                "submissionNumber": self.submission.number,
                "definitionName": definition_name,
                "justification": justification,
            },
        ))
