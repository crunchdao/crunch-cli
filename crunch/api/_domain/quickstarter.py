from dataclasses import dataclass
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent
from crunch.api._domain.enum_ import Language
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition


@dataclass(frozen=True)
class QuickstarterAuthor(JsonDataClassMixin):
    name: str
    link: str | None = None


@dataclass(frozen=True)
class QuickstarterFile(JsonDataClassMixin):
    name: str
    url: str
    github_url: str


@dataclass
class Quickstarter(ApiModel):
    id: int
    name: str
    title: str
    authors: list[QuickstarterAuthor]
    language: Language
    notebook: bool

    # only in the response of `get`, not of `list`
    files: list[QuickstarterFile] | None = None

    competition = Parent["Competition"]()

    def reload(self) -> "Quickstarter":
        return self._update_from(self.competition.quickstarters.get(self.name))


class QuickstarterCollection(ApiCollection[Quickstarter]):

    model = Quickstarter

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, quickstarter: Quickstarter) -> Quickstarter:
        quickstarter.competition = self.competition
        return quickstarter

    def get(self, /, name: str) -> Quickstarter:
        return self.from_dict(self.client.api.get_json([
            "v2", "competitions", self.competition.name, "quickstarters", name
        ]))

    def list(self) -> list[Quickstarter]:
        return self.from_dict_array(self.client.api.get_json([
            "v2", "competitions", self.competition.name, "quickstarters"
        ]))
