import enum
import typing
import warnings

from ..resource import Collection, Model
from .common import GpuRequirement

class LibraryListInclude(enum.Enum):

    ALL = "ALL"
    STANDARD = "STANDARD"
    THIRD_PARTY = "THIRD_PARTY"

    def __repr__(self):
        return self.name


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
        /,
        include: typing.Optional[LibraryListInclude] = None,
        name: typing.Optional[str] = None,
        gpu_requirement: typing.Optional[GpuRequirement] = None,
        standard: typing.Optional[bool] = None,
    ) -> typing.List[Library]:
        if include is not None:
            warnings.warn("The 'include' parameter is deprecated", DeprecationWarning)

            return self.prepare_models(
                self._client.api.list_libraries_v1(
                    include=include.name
                )
            )

        return self.prepare_models(
            self._client.api.list_libraries_v2(
                name=name,
                gpu_requirement=gpu_requirement,
                standard=standard,
            )
        )


class LibraryEndpointMixin:

    def list_libraries_v1(
        self,
        include
    ):
        params = {}
        if include is not None:
            params["include"] = include

        return self._result(
            self.get(
                "/v1/libraries",
                params=params
            ),
            json=True
        )

    def list_libraries_v2(
        self,
        name,
        gpu_requirement,
        standard,
    ):
        params = {}

        if name is not None:
            params["name"] = name

        if gpu_requirement is not None:
            params["gpuRequirement"] = gpu_requirement.name()

        if standard is not None:
            params["standard"] = str(standard).lower()
        
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
