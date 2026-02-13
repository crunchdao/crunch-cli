from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from dataclasses_json import LetterCase, Undefined, dataclass_json

from crunch.api.resource import Collection, Model

if TYPE_CHECKING:
    from crunch.api.client import Client
    from crunch.api.domain.competition import Competition
    from crunch.api.identifiers import CompetitionIdentifierType


class DataReleaseTargetResolution(Enum):

    ALREADY = "ALREADY"
    PENDING = "PENDING"
    RESOLVED = "RESOLVED"

    def __repr__(self):
        return self.name


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
class DataFile:

    name: str
    url: str
    size: int
    signed: bool
    compressed: bool


@dataclass(frozen=True)
class KnownData:

    X_TRAIN = "x_train"
    Y_TRAIN = "y_train"
    X_TEST = "x_test"
    Y_TEST = "y_test"
    X = "x"
    Y = "y"
    Y_RAW = "y_raw"
    EXAMPLE_PREDICTION = "example_prediction"


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE
)
@dataclass(frozen=True)
class DataFiles:

    x_train: DataFile
    x_test: DataFile
    y_train: DataFile
    y_test: DataFile
    example_prediction: DataFile

    def items(self):
        return vars(self).items()


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE
)
@dataclass(frozen=True)
class OriginalFiles:

    x: DataFile
    y: DataFile
    y_raw: Optional[DataFile]
    example_prediction: DataFile

    def items(self):
        return vars(self).items()


DataFilesUnion = Union[DataFiles, OriginalFiles, Dict[str, DataFile]]


class DataReleaseSplitGroup(Enum):

    TRAIN = "TRAIN"
    TEST = "TEST"

    def __repr__(self):
        return self.name


class DataReleaseSplitReduced(Enum):

    X = "X"
    XY = "XY"

    def __repr__(self):
        return self.name


SplitKeyPythonType = Union[str, int]


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
class DataReleaseSplit:

    key: SplitKeyPythonType
    group: DataReleaseSplitGroup
    reduced: Optional[DataReleaseSplitReduced] = None

    @staticmethod
    def from_dict_array(
        input: List[Dict[str, Any]]
    ):
        return [
            DataReleaseSplit.from_dict(x)
            for x in input
        ]


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
class DataReleaseFeature:

    group: str
    name: str

    @staticmethod
    def from_dict_array(
        input: List[Dict[str, Any]]
    ):
        return [
            DataReleaseFeature.from_dict(x)
            for x in input
        ]


class SizeVariant(Enum):

    DEFAULT = "DEFAULT"
    SMALL = "SMALL"
    LARGE = "LARGE"

    def __repr__(self):
        return self.name


class DataRelease(Model):

    resource_identifier_attribute = "number"

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional[Dict[str, Any]] = None,
        client: Optional["Client"] = None,
        collection: Optional["DataReleaseCollection"] = None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def id(self) -> int:
        return super().id  # type: ignore

    @property
    def competition(self):
        return self._competition

    @property
    def name(self) -> Optional[str]:
        return self._attrs["name"]

    @property
    def number(self) -> int:
        return self._attrs["number"]

    @property
    def embargo(self) -> int:
        return self._attrs["embargo"]

    @property
    def column_names(self) -> "ColumnNames":
        return ColumnNames.from_dict(self._attrs["columnNames"])

    @property
    def number_of_features(self) -> int:
        return self._attrs["numberOfFeatures"]

    @property
    def hash(self) -> Optional[str]:
        return self._attrs["hash"]

    @property
    def target_resolution(self):
        return DataReleaseTargetResolution[self._attrs["target_resolution"]]

    @property
    def data_files(self) -> DataFilesUnion:
        files = self._attrs.get("dataFiles")
        if not files:
            self.reload()
            files = self._attrs["dataFiles"]

        if "xTrain" in files and "yTrain" in files:
            return DataFiles.from_dict(files)

        if "x" in files and "y" in files:
            return OriginalFiles.from_dict(files)

        return MappingProxyType({
            key: DataFile.from_dict(value)
            for key, value in files.items()
        })

    @property
    def splits(self) -> List[DataReleaseSplit]:
        splits = self._attrs.get("splits")
        if splits is None:
            self.reload(include_splits=True)
            splits = self._attrs["splits"]

        return list(DataReleaseSplit.from_dict_array(splits))

    @property
    def default_feature_group(self) -> str:
        return self._attrs["defaultFeatureGroup"]

    @property
    def features(self) -> Tuple[DataReleaseFeature]:
        features = self._attrs.get("features")
        if features is None:
            self.reload(include_features=True)
            features = self._attrs["features"]

        return tuple(DataReleaseFeature.from_dict_array(features))

    def reload(
        self,
        include_splits: bool = True,
        include_features: bool = True,
    ):
        return super().reload(
            include_splits=include_splits,
            include_features=include_features,
        )


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE
)
@dataclass(frozen=True)
class TargetColumnNames:

    id: int
    name: str
    side: Optional[str]
    input: Optional[str]
    output: Optional[str]
    file_path: Optional[str]


@dataclass_json(
    letter_case=LetterCase.CAMEL,
    undefined=Undefined.EXCLUDE
)
@dataclass(frozen=True)
class ColumnNames:

    id: str
    moon: str
    side: Optional[str]
    input: Optional[str]
    output: Optional[str]
    targets: List[TargetColumnNames]

    @property
    def first_target(self):
        return next(iter(self.targets), None)

    @property
    def inputs(self):
        return [
            target_column_names.input
            for target_column_names in self.targets
        ]

    @property
    def outputs(self):
        return [
            target_column_names.output
            for target_column_names in self.targets
        ]

    @property
    def target_names(self):
        return [
            target_column_names.name
            for target_column_names in self.targets
        ]

    def get_target_by_name(self, name: str):
        for target in self.targets:
            if target.name == name:
                return target

        return None


class DataReleaseCollection(Collection[DataRelease]):

    model = DataRelease

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> Iterator[DataRelease]:
        return super().__iter__()

    def get(
        self,
        number: Union[int, str],
        include_splits: bool = True,
        include_features: bool = True,
    ) -> DataRelease:
        return self.prepare_model(
            self._client.api.get_data_release(
                self.competition.id,
                number,
                include_splits=include_splits,
                include_features=include_features,
            )
        )

    def list(
        self
    ) -> List[DataRelease]:
        return self.prepare_models(
            self._client.api.list_data_releases(
                self.competition.id
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition
        )


class DataReleaseEndpointMixin:

    def list_data_releases(
        self,
        competition_identifier: "CompetitionIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/data-releases"
            ),
            json=True
        )

    def get_data_release(
        self,
        competition_identifier: "CompetitionIdentifierType",
        number: int,
        include_splits: bool = False,
        include_features: bool = False,
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/data-releases/{number}",
                params={
                    "includeSplits": include_splits,
                    "includeFeatures": include_features,
                }
            ),
            json=True
        )
