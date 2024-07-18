import dataclasses
import enum
import types
import typing

import dataclasses_json

from ..resource import Collection, Model
from .competition import Competition


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
    y_raw: typing.Optional[DataFile]
    example_prediction: DataFile
    orthogonalization_data: typing.Optional[DataFile]


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

    @staticmethod
    def from_dict_array(
        input: typing.List[dict]
    ):
        return [
            DataReleaseSplit.from_dict(x)
            for x in input
        ]


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class DataReleaseFeature:

    group: str
    name: str

    @staticmethod
    def from_dict_array(
        input: typing.List[dict]
    ):
        return [
            DataReleaseFeature.from_dict(x)
            for x in input
        ]


class DataRelease(Model):

    resource_identifier_attribute = "number"

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
        return self._attrs["name"]

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
    def hash(self) -> typing.Optional[str]:
        return self._attrs["hash"]

    @property
    def target_resolution(self):
        return DataReleaseTargetResolution[self._attrs["target_resolution"]]

    @property
    def data_files(self) -> typing.Union[DataFiles, OriginalFiles, typing.Dict[str, DataFile]]:
        files = self._attrs.get("dataFiles")
        if not files:
            self.reload()
            files = self._attrs["dataFiles"]

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
        splits = self._attrs.get("splits")
        if splits is None:
            self.reload(include_splits=True)
            splits = self._attrs["splits"]

        return tuple(DataReleaseSplit.from_dict_array(splits))

    @property
    def default_feature_group(self) -> str:
        return self._attrs["defaultFeatureGroup"]

    @property
    def features(self) -> typing.Tuple[DataReleaseFeature]:
        features = self._attrs.get("features")
        if features is None:
            self.reload(include_features=True)
            features = self._attrs["features"]

        return tuple(DataReleaseFeature.from_dict_array(features))

    def reload(
        self,
        include_splits=True,
        include_features=True,
    ):
        return super().reload(
            include_splits=include_splits,
            include_features=include_features,
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class TargetColumnNames:

    id: int
    name: str
    input: str
    output: str

    @property
    def merge_keys(self):
        input = self.input
        output = self.output

        if input == output:
            input += "_x"
            output += "_y"
        
        return (
            input,
            output,
        )

@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class ColumnNames:

    id: str
    moon: str
    targets: typing.List[TargetColumnNames]

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
        include_splits=True,
        include_features=True,
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
    ) -> typing.List[DataRelease]:
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
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/data-releases"
            ),
            json=True
        )

    def get_data_release(
        self,
        competition_identifier,
        number,
        include_splits=False,
        include_features=False,
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
