from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple, Union

from crunch.api._domain.common import GpuRequirement
from crunch.api._domain.enum_ import Language
from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch_convert.notebook import ImportedRequirementLanguage

    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Library(Model[int]):

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def standard(self) -> bool:
        return self._attrs["standard"]

    @property
    def gpu_requirement(self):
        return GpuRequirement[self._attrs["gpuRequirement"]]

    @property
    def language(self):
        return Language[self._attrs["language"]]

    @property
    def aliases(self) -> Tuple[str]:
        return tuple(self._attrs.get("aliases") or [])


class LibraryCollection(Collection[Library]):

    model = Library

    def list(
        self,
        *,
        name: Optional[str] = None,
        gpu_requirement: Optional[GpuRequirement] = None,
        standard: Optional[bool] = None,
        language: Optional[Union["Language", "ImportedRequirementLanguage"]] = None,
    ) -> Iterable[Library]:
        return self.prepare_models(
            self._checked_client.api.list_libraries_v2(
                name=name,
                gpu_requirement=gpu_requirement,
                standard=standard,
                language=language,
            )
        )


class LibraryRequestVerdict(Enum):

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    def __repr__(self):
        return self.name


class LibraryRequest(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["LibraryRequestCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def competition(self):
        return self._competition

    @property
    def language(self):
        return Language[self._attrs["language"]]

    @property
    def name(self) -> Optional[str]:
        return self._attrs.get("name")

    @property
    def alias(self) -> Optional[str]:
        return self._attrs.get("alias")

    @property
    def gpu_requirement(self) -> Optional[GpuRequirement]:
        gpu_requirement = self._attrs.get("gpuRequirement")
        return GpuRequirement[gpu_requirement] if gpu_requirement else None

    @property
    def additional_details(self) -> Optional[str]:
        return self._attrs.get("additionalDetails")

    @property
    def verdict(self):
        return LibraryRequestVerdict[self._attrs["verdict"]]

    @property
    def verdict_reason(self) -> Optional[str]:
        return self._attrs.get("verdictReason")

    @property
    def verdict_at(self) -> Optional[datetime]:
        verdict_at = self._attrs.get("verdictAt")
        return datetime.fromisoformat(verdict_at) if verdict_at else None

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])


class LibraryRequestCollection(Collection[LibraryRequest]):

    model = LibraryRequest

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.competition = competition

    def create(
        self,
        *,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        language: Optional[Union["Language", "ImportedRequirementLanguage"]] = None,
        gpu_requirement: Optional[GpuRequirement] = None,
        additional_details: Optional[str] = None,
    ) -> LibraryRequest:
        return self.prepare_model(
            self._checked_client.api.create_library_request(
                competition_id=self.competition.id,
                name=name,
                alias=alias,
                language=language,
                gpu_requirement=gpu_requirement,
                additional_details=additional_details,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", LibraryRequest], *args: Any) -> LibraryRequest:
        return super().prepare_model(
            attrs,
            self.competition,
            *args
        )


class LibraryEndpointMixin(EndpointMixin):

    def list_libraries_v2(
        self,
        name: Optional[str],
        gpu_requirement: Optional[GpuRequirement],
        standard: Optional[bool],
        language: Optional[Union["Language", "ImportedRequirementLanguage"]],
    ):
        params: Dict[str, Any] = {}

        if name is not None:
            params["name"] = name

        if gpu_requirement is not None:
            params["gpuRequirement"] = gpu_requirement.name

        if standard is not None:
            params["standard"] = str(standard).lower()

        if language is not None:
            params["language"] = language.name

        return self._paginated(
            lambda page_request: self.get(
                "/v2/libraries",
                params={
                    **params,
                    "page": page_request.number,
                    "size": page_request.size,
                }
            ),
            page_size=1000
        )

    def create_library_request(
        self,
        competition_id: int,
        name: Optional[str],
        alias: Optional[str],
        language: Optional[Union["Language", "ImportedRequirementLanguage"]],
        gpu_requirement: Optional[GpuRequirement],
        additional_details: Optional[str],
    ):
        body: Dict[str, Any] = {
            "competitionId": competition_id,
            "name": name,
            "alias": alias,
            "gpuRequirement": gpu_requirement.name if gpu_requirement is not None else None,
            "additionalDetails": additional_details,
        }

        if language is not None:
            body["language"] = language.name

        return self._result(
            self.post(
                "/v1/library-requests",
                json=body,
            ),
            json=True
        )
