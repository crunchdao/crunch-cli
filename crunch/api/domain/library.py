import enum
import typing

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
        include: typing.Optional[LibraryListInclude] = LibraryListInclude.ALL
    ) -> typing.List[Library]:
        return self.prepare_models(
            self._client.api.list_libraries(
                include=include.name
            )
        )


class LibraryEndpointMixin:

    def list_libraries(
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
