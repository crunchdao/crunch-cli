import os
import typing
import dataclasses

import click

from .. import constants, utils, api, container


@dataclasses.dataclass
class DataFile:

    url: str
    path: str
    size: int
    signed: bool

    @property
    def has_size(self):
        return self.size != -1


def _get_data_urls(
    round: api.Round,
    data_directory: str,
) -> typing.Tuple[
    int,
    int,
    typing.List[int],
    container.Features,
    api.ColumnNames,
    typing.Tuple[DataFile, DataFile, DataFile, DataFile, DataFile]
]:
    data_release = round.phases.get_submission().get_data_release()

    embargo = data_release.embargo
    number_of_features = data_release.number_of_features
    column_names = data_release.column_names
    data_files: api.DataFiles = data_release.data_files
    splits = data_release.splits
    features = container.Features.from_data_release(data_release)

    split_keys = [
        split.key
        for split in splits
        if (
            split.group == api.DataReleaseSplitGroup.TEST
            and split.reduced is not None
        )
    ]

    def get_file(file_name: str) -> DataFile:
        key = file_name.lower()
        data_file: api.DataFile = getattr(data_files, key)

        url = data_file.url
        path = os.path.join(
            data_directory,
            f"{file_name}.{utils.get_extension(url)}"
        )

        return DataFile(
            url,
            path,
            data_file.size,
            data_file.signed
        )

    x_train = get_file("X_train")
    y_train = get_file("y_train")
    x_test = get_file("X_test")
    y_test = get_file("y_test")
    example_prediction = get_file("example_prediction")

    return (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        (
            x_train,
            y_train,
            x_test,
            y_test,
            example_prediction
        )
    )


def _download(
    data_file: DataFile,
    force: bool
):
    if data_file is None:
        return

    file_length_str = f" ({data_file.size} bytes)" if data_file.has_size else ""
    print(f"download {data_file.path} from {utils.cut_url(data_file.url)}" + file_length_str)

    if not data_file.has_size:
        print(f"skip: not given by server")
        return

    exists = os.path.exists(data_file.path)
    if not force and exists:
        stat = os.stat(data_file.path)
        if stat.st_size == data_file.size:
            print(f"already exists: file length match")
            return

    if not data_file.signed:
        print(f"signature missing: cannot download file without being authenticated")
        raise click.Abort()

    utils.download(data_file.url, data_file.path, log=False)


def download(
    round_number="@current",
    force=False,
):
    _, project = api.Client.from_project()

    competition = project.competition
    round = competition.rounds.get(round_number)

    os.makedirs(constants.DOT_DATA_DIRECTORY, exist_ok=True)

    (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        (
            x_train,
            y_train,
            x_test,
            y_test,
            example_prediction,
        )
    ) = _get_data_urls(
        round,
        constants.DOT_DATA_DIRECTORY,
    )

    _download(x_train, force)
    _download(y_train, force)
    _download(x_test, force)
    _download(y_test, force)
    _download(example_prediction, force)

    return (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        (
            x_train.path,
            y_train.path,
            x_test.path,
            y_test.path if y_test.has_size else None,
            example_prediction.path,
        )
    )


def download_no_data_available():
    print("\n---")
    print("No data is available yet")
