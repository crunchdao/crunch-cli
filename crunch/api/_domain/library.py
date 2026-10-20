from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._domain.common import GpuRequirement
from crunch.api._domain.enum_ import Language

if TYPE_CHECKING:
    from crunch_convert.notebook import ImportedRequirementLanguage

    from crunch.api._domain.competition import Competition


@dataclass
class Library(ApiModel):
    id: int
    name: str
    language: Language
    standard: bool
    gpu_requirement: GpuRequirement
    aliases: list[str]


class LibraryCollection(ApiCollection[Library]):

    model = Library

    def iter(
        self,
        *,
        name: str | None = None,
        gpu_requirement: GpuRequirement | None = None,
        standard: bool | None = None,
        language: "Language | ImportedRequirementLanguage | None" = None,
    ) -> Iterator[Library]:
        filters: dict[str, Any] = {
            "name": name,
            "gpuRequirement": gpu_requirement.name if gpu_requirement is not None else None,
            "standard": str(standard).lower() if standard is not None else None,
            "language": language.name if language is not None else None,
        }

        return self.from_dict_iterator(self.client.api.paginate_json(
            ["v2", "libraries"],
            params=filters,
            page_size=1000,
        ))


class LibraryRequestVerdict(ApiEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class LibraryRequest(ApiModel):
    id: int
    language: Language
    verdict: LibraryRequestVerdict
    created_at: datetime
    name: str | None = None
    alias: str | None = None
    gpu_requirement: GpuRequirement | None = None
    additional_details: str | None = None
    verdict_reason: str | None = None
    verdict_at: datetime | None = None

    competition = Parent["Competition"]()


class LibraryRequestCollection(ApiCollection[LibraryRequest]):

    model = LibraryRequest

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, library_request: LibraryRequest) -> LibraryRequest:
        library_request.competition = self.competition
        return library_request

    def create(
        self,
        *,
        name: str | None = None,
        alias: str | None = None,
        language: "Language | ImportedRequirementLanguage | None" = None,
        gpu_requirement: GpuRequirement | None = None,
        additional_details: str | None = None,
    ) -> LibraryRequest:
        body: dict[str, Any] = {
            "competitionId": self.competition.id,
            "name": name,
            "alias": alias,
            "gpuRequirement": gpu_requirement.name if gpu_requirement is not None else None,
            "additionalDetails": additional_details,
        }

        if language is not None:
            body["language"] = language.name

        return self.from_dict(self.client.api.post_json(
            ["v1", "library-requests"],
            json=body,
        ))
