import dataclasses
import typing

import dataclasses_json

from ..resource import Collection, Model
from .competition import Competition, CompetitionFormat
from .enum_ import Language


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class QuickstarterAuthor:

    name: str
    link: typing.Optional[str] = None

    @staticmethod
    def from_dict_array(
        input: typing.List[dict]
    ):
        return [
            QuickstarterAuthor.from_dict(x)
            for x in input
        ]


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class QuickstarterFile:

    name: str
    url: str
    github_url: str
    colab_url: typing.Optional[str] = None

    @staticmethod
    def from_dict_array(
        input: typing.List[dict]
    ):
        return [
            QuickstarterFile.from_dict(x)
            for x in input
        ]


class Quickstarter(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        competition: typing.Optional[Competition],
        competition_format: typing.Optional[CompetitionFormat],
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        assert (
            (competition is None and competition_format is not None)
            or (competition is not None and competition_format is None)
        )

        self._competition = competition
        self._competition_format = competition_format

    @property
    def competition(self):
        return self._competition

    @property
    def generic(self):
        return self.competition is None

    @property
    def name(self) -> bool:
        return self._attrs["name"]

    @property
    def title(self) -> bool:
        return self._attrs["title"]

    @property
    def authors(self) -> typing.List[QuickstarterAuthor]:
        return QuickstarterAuthor.from_dict_array(self._attrs["authors"])

    @property
    def language(self) -> Language:
        return Language[self._attrs["language"]]

    @property
    def notebook(self) -> bool:
        return self._attrs["notebook"]

    @property
    def files(self) -> typing.List[QuickstarterFile]:
        files = self._attrs.get("files")
        if not files:
            self.reload()
            files = self._attrs["files"]

        return QuickstarterFile.from_dict_array(files)


class QuickstarterCollection(Collection):

    model = Quickstarter

    def __init__(
        self,
        competition: typing.Optional[Competition],
        competition_format: typing.Optional[CompetitionFormat],
        client=None
    ):
        super().__init__(client)

        assert (
            (competition is None and competition_format is not None)
            or (competition is not None and competition_format is None)
        )

        self.competition = competition
        self.competition_format = competition_format

    def __iter__(self) -> typing.Iterator[Quickstarter]:
        return super().__iter__()

    def get(
        self,
        quickstarter_name: str
    ) -> Quickstarter:
        return self.prepare_model(
            self._client.api.get_quickstarter(
                self.competition.resource_identifier if self.competition else self.competition_format,
                quickstarter_name
            )
        )

    def list(
        self
    ) -> typing.List[Quickstarter]:
        return self.prepare_models(
            self._client.api.list_quickstarters(
                self.competition.resource_identifier if self.competition else self.competition_format
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition,
            self.competition_format,
        )


def _get_quickstarter_part(
    competition_identifier: typing.Optional[str]
):
    if isinstance(competition_identifier, CompetitionFormat):
        return f"generic/{competition_identifier.name}"

    return f"competitions/{competition_identifier}"


class QuickstarterEndpointMixin:

    def list_quickstarters(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/quickstarters/{_get_quickstarter_part(competition_identifier)}"
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
                f"/v1/quickstarters/{_get_quickstarter_part(competition_identifier)}/{quickstarter_name}"
            ),
            json=True
        )
