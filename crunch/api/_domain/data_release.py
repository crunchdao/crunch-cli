from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeAlias

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition


@dataclass(frozen=True)
class DataFile(JsonDataClassMixin):
    name: str
    url: str = field(repr=False)
    size: int
    signed: bool
    compressed: bool


DataFiles: TypeAlias = dict[str, DataFile]


class SizeVariant(ApiEnum):
    DEFAULT = "DEFAULT"
    SMALL = "SMALL"
    LARGE = "LARGE"


@dataclass
class DataRelease(ApiModel):
    id: int
    number: int
    name: str
    embargo: int
    number_of_features: int
    hash: str | None

    # only in the detailed response
    data_files: DataFiles | None = None

    competition = Parent["Competition"]()

    def reload(self) -> "DataRelease":
        return self._update_from(self.competition.data_releases.get(self.number))


class DataReleaseCollection(ApiCollection[DataRelease]):

    model = DataRelease

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, data_release: DataRelease) -> DataRelease:
        data_release.competition = self.competition
        return data_release

    def get(self, /, number: int) -> DataRelease:
        return self.from_dict(self.client.api.get_json(
            ["v1", "competitions", self.competition.id, "data-releases", number],
        ))

    def list(self) -> list[DataRelease]:
        return self.from_dict_array(self.client.api.get_json(
            ["v1", "competitions", self.competition.id, "data-releases"]
        ))
