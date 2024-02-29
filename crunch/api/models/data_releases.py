import dataclasses
import enum
import types
import typing

import dataclasses_json

from .competitions import Competition
from .resource import Collection, Model


class DataReleaseTargetResolution(enum.Enum):

    ALREADY = "ALREADY"
    PENDING = "PENDING"
    RESOLVED = "RESOLVED"

    def __repr__(self):
        return self.name


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class DataFile:

    url: str
    size: int
    signed: bool


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class DataFiles:

    x_train: DataFile
    x_test: DataFile
    y_train: DataFile
    y_test: DataFile
    example_prediction: DataFile


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class OriginalFiles:

    x: DataFile
    y: DataFile
    example_prediction: DataFile


class DataReleaseSplitGroup(enum.Enum):

    TRAIN = "TRAIN"
    TEST = "TEST"

    def __repr__(self):
        return self.name


class DataReleaseSplitReduced(enum.Enum):

    X = "X"
    XY = "XY"

    def __repr__(self):
        return self.name


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class DataReleaseSplit:

    key: typing.Union[str, int]
    group: DataReleaseSplitGroup
    reduced: typing.Optional[DataReleaseSplitReduced] = None


class DataRelease(Model):

    id_attribute = "number"

    def __init__(
        self,
        competition: Competition,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def competition(self):
        return self._competition

    @property
    def name(self) -> typing.Optional[str]:
        return self.attrs["name"]

    @property
    def embargo(self) -> int:
        return self.attrs["embargo"]

    @property
    def number_of_features(self) -> int:
        return self.attrs["numberOfFeatures"]

    @property
    def hash(self) -> typing.Optional[str]:
        return self.attrs["hash"]

    @property
    def target_resolution(self):
        return DataReleaseTargetResolution[self.attrs["target_resolution"]]

    @property
    def data_files(self) -> typing.Union[DataFiles, OriginalFiles, typing.Dict[str, DataFile]]:
        files = self.attrs.get("dataFiles")
        if not files:
            self.reload()
            files = self.attrs["dataFiles"]

        if "xTrain" in files:
            return DataFiles.from_dict(files)

        if "x" in files:
            return OriginalFiles.from_dict(files)

        return types.MappingProxyType({
            key: DataFile.from_dict(value)
            for key, value in files.items()
        })

    @property
    def splits(self) -> typing.Tuple[DataReleaseSplit]:
        splits = self.attrs.get("splits")
        if not splits:
            self.reload(include_splits=True)
            splits = self.attrs["splits"]

        return tuple(
            DataReleaseSplit.from_dict(split)
            for split in splits
        )

    def reload(
        self,
        include_splits: bool = False
    ):
        return super().reload(
            include_splits=include_splits
        )


class DataReleaseCollection(Collection):

    model = DataRelease

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[DataRelease]:
        return super().__iter__()

    def get(
        self,
        number: typing.Union[int, str],
        include_splits: bool = False
    ) -> DataRelease:
        response = self.client.api.get_data_release(
            self.competition.id,
            number,
            include_splits=include_splits
        )

        return self.prepare_model(
            response,
            self.competition
        )

    def list(
        self
    ) -> typing.List[Competition]:
        response = self.client.api.list_data_releases(
            self.competition.id
        )

        return [
            self.prepare_model(
                item,
                self.competition
            )
            for item in response
        ]
