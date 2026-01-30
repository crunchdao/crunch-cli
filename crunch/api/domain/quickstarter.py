import dataclasses
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

import dataclasses_json

from crunch.api.domain.enum_ import Language
from crunch.api.resource import Collection, Model

if TYPE_CHECKING:
    from crunch.api.client import Client
    from crunch.api.domain.competition import Competition
    from crunch.api.types import Attrs


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,  # type: ignore
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class QuickstarterAuthor:

    name: str
    link: Optional[str] = None

    @staticmethod
    def from_dict_array(
        input: List[Dict[str, Any]],
    ) -> List["QuickstarterAuthor"]:
        return [
            QuickstarterAuthor.from_dict(x)  # type: ignore
            for x in input
        ]


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,  # type: ignore
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class QuickstarterFile:

    name: str
    url: str
    github_url: str
    colab_url: Optional[str] = None

    @staticmethod
    def from_dict_array(
        input: List[Dict[str, Any]],
    ) -> List["QuickstarterFile"]:
        return [
            QuickstarterFile.from_dict(x)  # type: ignore
            for x in input
        ]


class Quickstarter(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["QuickstarterCollection"] = None,
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def competition(self):
        return self._competition

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def title(self) -> str:
        return self._attrs["title"]

    @property
    def authors(self) -> List[QuickstarterAuthor]:
        return QuickstarterAuthor.from_dict_array(self._attrs["authors"])

    @property
    def language(self) -> Language:
        return Language[self._attrs["language"]]

    @property
    def notebook(self) -> bool:
        return self._attrs["notebook"]

    @property
    def files(self) -> List[QuickstarterFile]:
        files = self._attrs.get("files")
        if not files:
            self.reload()
            files = self._attrs["files"]

        return QuickstarterFile.from_dict_array(files)


class QuickstarterCollection(Collection):

    model = Quickstarter

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> Iterator[Quickstarter]:
        return super().__iter__()  # type: ignore

    def get(
        self,
        quickstarter_name: str
    ) -> Quickstarter:
        return self.prepare_model(
            self._client.api.get_quickstarter(
                self.competition.resource_identifier,
                quickstarter_name
            )
        )

    def list(
        self
    ) -> List[Quickstarter]:
        return self.prepare_models(
            self._client.api.list_quickstarters(
                self.competition.resource_identifier
            )
        )

    def prepare_model(self, attrs: "Attrs"):
        return super().prepare_model(
            attrs,
            self.competition,
        )


class QuickstarterEndpointMixin:

    def list_quickstarters(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/quickstarters"
            ),
            json=True
        )

    def get_quickstarter(
        self,
        competition_identifier,
        quickstarter_name
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/quickstarters/{quickstarter_name}"
            ),
            json=True
        )
