import typing

from ..resource import Collection, Model
from .common import GpuRequirement

if typing.TYPE_CHECKING:
    from ...convert import ImportedRequirement, ImportedRequirementLanguage
    from .enum_ import Language


class Library(Model):

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
    def aliases(self) -> typing.Tuple[str]:
        return tuple(self._attrs.get("aliases") or [])


class LibraryCollection(Collection):

    model = Library

    def __iter__(self) -> typing.Iterator[Library]:
        return super().__iter__()

    def list(
        self,
        *,
        name: typing.Optional[str] = None,
        gpu_requirement: typing.Optional[GpuRequirement] = None,
        standard: typing.Optional[bool] = None,
        language: typing.Optional[typing.Union["Language", "ImportedRequirementLanguage"]] = None,
    ) -> typing.List[Library]:
        return self.prepare_models(
            self._client.api.list_libraries_v2(
                name=name,
                gpu_requirement=gpu_requirement,
                standard=standard,
                language=language,
            )
        )


class LibraryEndpointMixin:

    def list_libraries_v2(
        self,
        name,
        gpu_requirement,
        standard,
        language,
    ):
        params = {}

        if name is not None:
            params["name"] = name

        if gpu_requirement is not None:
            params["gpuRequirement"] = gpu_requirement.name()

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
