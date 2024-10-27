import dataclasses
import os
import typing

import click

from . import api, constants, container, utils

# TODO Remove me
LEGACY_NAME_MAPPING = {
    "x_train": "X_train",
    "y_train": "y_train",
    "x_test": "X_test",
    "y_test": "y_test",
    "example_prediction": "example_prediction",
}


@dataclasses.dataclass
class PreparedDataFile:

    path: str
    url: str
    size: int
    signed: bool

    @property
    def has_size(self):
        return self.size != -1


def prepare_all(
    data_directory_path: str,
    data_files: api.DataFilesUnion,
):
    return {
        key: prepare_one(data_directory_path, value, key)
        for key, value in data_files.items()
    }


def prepare_one(
    data_directory_path: str,
    data_file: api.DataFile,
    key: str
):
    url = data_file.url
    path = os.path.join(
        data_directory_path,
        data_file.name or (f"{LEGACY_NAME_MAPPING[key]}.{utils.get_extension(url)}")
    )

    return PreparedDataFile(
        path,
        url,
        data_file.size,
        data_file.signed
    )


def save_one(
    data_file: PreparedDataFile,
    force: bool,
    print=print,
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


def save_all(
    data_files: typing.Dict[str, PreparedDataFile],
    force: bool,
    print=print,
):
    for data_file in data_files.values():
        save_one(data_file, force, print)

    return {
        key: value.path
        for key, value in data_files.items()
        if value.has_size
    }
