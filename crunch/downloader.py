import dataclasses
import os
import shutil
import subprocess
import tempfile
import typing
import zipfile

import click

from . import api, constants, utils

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

    @property
    def uncompressed_marker_path(self):
        file_name = os.path.basename(self.path)
        parent_directory_path = os.path.dirname(self.path)

        return os.path.join(
            parent_directory_path,
            f".{file_name}.uncompressed"
        )


def delete_other_uncompressed_markers(
    data_directory_path: str,
    data_files: typing.List[PreparedDataFile],
):
    expected_markers = {
        data_file.uncompressed_marker_path
        for data_file in data_files
    }

    found_markers: typing.Set[str] = set()
    for root, _, files in os.walk(data_directory_path, topdown=False):
        for file in files:
            if file.startswith(".") and file.endswith(".uncompressed"):
                path = os.path.join(root, file)

                found_markers.add(path)

    useless_markers = found_markers - expected_markers
    for marker in useless_markers:
        os.unlink(marker)


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
    data_file: typing.Optional[PreparedDataFile],
    force: bool,
    print: typing.Callable[[typing.Any], None] = print,
    progress_bar: bool = True,
):
    if data_file is None:
        return

    file_size = data_file.size
    file_path = data_file.path
    file_name = os.path.basename(file_path)
    parent_directory_path = os.path.dirname(file_path)

    uncompressed_marker = data_file.uncompressed_marker_path

    local_size = _read_size(file_path, uncompressed_marker)

    def download():
        file_length_str = f" ({file_size} bytes)" if data_file.has_size else ""
        print(f"{file_path}: download from {utils.cut_url(data_file.url)}" + file_length_str)

        if not data_file.has_size:
            print(f"{file_path}: skip, not given by server")
            return None

        if not force and local_size == file_size:
            print(f"{file_path}: already exists, file length match")
            return False

        if not data_file.signed:
            print(f"{file_path}: signature missing, cannot download file without being authenticated")
            raise click.Abort()

        utils.download(data_file.url, file_path, log=False, progress_bar=progress_bar)
        return True

    has_new_content = download()
    if has_new_content is None:
        return

    if not data_file.compressed:
        return

    if os.path.exists(uncompressed_marker):
        if has_new_content:
            os.unlink(uncompressed_marker)
        elif not force:
            print(f"{file_path}: already uncompressed, marker is present")
            return

    with tempfile.TemporaryDirectory(
        prefix=f"{file_name}.",
        dir=parent_directory_path
    ) as temporary_directory_path:
        print(f"{file_path}: uncompress into {temporary_directory_path}")
        _uncompress(file_path, temporary_directory_path)

        for name in os.listdir(temporary_directory_path):
            if name in constants.MACOS_HIDDEN_FILES:
                continue

            source_path = os.path.join(temporary_directory_path, name)
            destination_path = os.path.join(parent_directory_path, name)

            if os.path.exists(destination_path):
                if os.path.isdir(destination_path):
                    shutil.rmtree(destination_path)
                else:
                    os.unlink(destination_path)

            shutil.move(source_path, parent_directory_path)

    with open(uncompressed_marker, 'w') as fd:
        # TODO Should include name of unzipped files as well for later cleanup
        fd.write(str(file_size))

    os.unlink(file_path)


def save_all(
    data_files: typing.Dict[str, PreparedDataFile],
    force: bool,
    print: typing.Callable[[str], None] = print,
    progress_bar: bool = True,
):
    for data_file in data_files.values():
        save_one(data_file, force, print, progress_bar)

    return {
        key: value.path
        for key, value in data_files.items()
        if value.has_size
    }


def _read_size(
    file_path: str,
    marker_file_path: str
):
    try:
        with open(marker_file_path, "r") as fd:
            content = fd.read()

        return int(content)
    except (FileNotFoundError, ValueError):
        pass

    try:
        stat = os.stat(file_path)

        return stat.st_size
    except FileNotFoundError:
        pass

    return None


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
