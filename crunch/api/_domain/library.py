from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple, Union

from crunch.api._domain.common import GpuRequirement
from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch_convert.notebook import ImportedRequirementLanguage

    from crunch.api._domain.enum_ import Language


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
