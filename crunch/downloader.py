import dataclasses
import os
import shutil
import subprocess
import tempfile
import typing
import zipfile

import click

from . import api, utils

# TODO Remove me
LEGACY_NAME_MAPPING = {
    "x_train": "X_train",
    "y_train": "y_train",
    "x_test": "X_test",
    "y_test": "y_test",
    "x": "X",
    "y": "y",
    "example_prediction": "example_prediction",
}


@dataclasses.dataclass
class PreparedDataFile:

    path: str
    url: str
    size: int
    signed: bool
    compressed: bool

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
        if value is not None
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
        data_file.signed,
        data_file.compressed,
    )


def save_one(
    data_file: PreparedDataFile,
    force: bool,
    print=print,
    progress_bar=True,
):
    if data_file is None:
        return

    def download():
        file_length_str = f" ({data_file.size} bytes)" if data_file.has_size else ""
        print(f"{data_file.path}: download from {utils.cut_url(data_file.url)}" + file_length_str)

        if not data_file.has_size:
            print(f"{data_file.path}: skip, not given by server")
            return None

        exists = os.path.exists(data_file.path)
        if not force and exists:
            stat = os.stat(data_file.path)
            if stat.st_size == data_file.size:
                print(f"{data_file.path}: already exists, file length match")
                return False

        if not data_file.signed:
            print(f"{data_file.path}: signature missing, cannot download file without being authenticated")
            raise click.Abort()

        utils.download(data_file.url, data_file.path, log=False, progress_bar=progress_bar)
        return True

    has_new_content = download()
    if has_new_content is None:
        return

    if not data_file.compressed:
        return

    zip_file_path = data_file.path
    zip_file_name = os.path.basename(zip_file_path)
    zip_parent_directory_path = os.path.dirname(zip_file_path)

    uncompressed_marker = os.path.join(
        zip_parent_directory_path,
        f".{zip_file_name}.uncompressed"
    )

    if os.path.exists(uncompressed_marker):
        if has_new_content:
            os.unlink(uncompressed_marker)
        elif not force:
            print(f"{zip_file_path}: already uncompressed, marker is present")
            return

    with tempfile.TemporaryDirectory(
        prefix=f"{zip_file_name}.",
        dir=zip_parent_directory_path
    ) as temporary_directory_path:
        print(f"{zip_file_path}: uncompress into {temporary_directory_path}")
        _uncompress(zip_file_path, temporary_directory_path)

        for name in os.listdir(temporary_directory_path):
            if name == "__MACOSX":
                continue

            source_path = os.path.join(temporary_directory_path, name)
            destination_path = os.path.join(zip_parent_directory_path, name)

            if os.path.exists(destination_path):
                shutil.rmtree(destination_path)

            shutil.move(source_path, zip_parent_directory_path)

        open(uncompressed_marker, 'w').close()


def _uncompress(
    zip_file_path: str,
    output_directory_path: str,
):
    unzip = shutil.which("unzip")

    if unzip:
        subprocess.call([
            unzip,
            "-q",
            "-d", output_directory_path,
            zip_file_path
        ])
    else:
        with zipfile.ZipFile(zip_file_path, "r") as zipfd:
            zipfd.extractall(output_directory_path)


def save_all(
    data_files: typing.Dict[str, PreparedDataFile],
    force: bool,
    print=print,
    progress_bar=True,
):
    for data_file in data_files.values():
        save_one(data_file, force, print, True)

    return {
        key: value.path
        for key, value in data_files.items()
        if value.has_size
    }
