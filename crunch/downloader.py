import logging
import os
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from crunch.api import DataFile
from crunch.download import _delete_other_uncompressed_markers, _download_data_file, _to_uncompressed_marker_path

if TYPE_CHECKING:
    from crunch.api import DataFiles

__all__ = [
    "PreparedDataFile",
    "prepare_all",
    "prepare_one",
    "save_one",
    "save_all",
    "delete_other_uncompressed_markers",
]


@dataclass
class PreparedDataFile:
    """Deprecated, use `crunch.download.download_data_files` instead."""

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
        return _to_uncompressed_marker_path(self.path)


def prepare_all(
    data_directory_path: str,
    data_files: "DataFiles",
):
    """Deprecated, use `crunch.download.download_data_files` instead."""

    _warn_deprecated("prepare_all", "download_data_files")

    return {
        key: _prepare(data_directory_path, value)
        for key, value in data_files.items()
    }


def prepare_one(
    data_directory_path: str,
    data_file: "DataFile",
):
    """Deprecated, use `crunch.download.download_data_files` instead."""

    _warn_deprecated("prepare_one", "download_data_files")

    return _prepare(data_directory_path, data_file)


def save_one(
    data_file: PreparedDataFile | None,
    force: bool,
    print: Callable[[Any], None] = print,
    progress_bar: bool = True,
):
    """Deprecated, use `crunch.download.download_data_files` instead."""

    _warn_deprecated("save_one", "download_data_files")

    if data_file is None:
        return

    _download_data_file(
        _to_data_file(data_file),
        data_file.path,
        force=force,
        logger=_to_logger(print),
        verbose=True,
        progress_bar=progress_bar,
    )


def save_all(
    data_files: dict[str, PreparedDataFile],
    force: bool,
    print: Callable[[str], Any] = print,
    progress_bar: bool = True,
):
    """Deprecated, use `crunch.download.download_data_files` instead."""

    _warn_deprecated("save_all", "download_data_files")

    logger = _to_logger(print)

    for data_file in data_files.values():
        _download_data_file(
            _to_data_file(data_file),
            data_file.path,
            force=force,
            logger=logger,
            verbose=True,
            progress_bar=progress_bar,
        )

    return {
        key: value.path
        for key, value in data_files.items()
        if value.has_size
    }


def delete_other_uncompressed_markers(
    data_directory_path: str,
    data_files: list[PreparedDataFile],
):
    """Deprecated, use `crunch.download.download_data_files` with `delete_other_uncompressed_markers=True` instead."""

    _warn_deprecated("delete_other_uncompressed_markers", "download_data_files")

    _delete_other_uncompressed_markers(
        data_directory_path,
        expected_marker_paths={
            data_file.uncompressed_marker_path
            for data_file in data_files
        },
    )


def _prepare(
    data_directory_path: str,
    data_file: "DataFile",
):
    return PreparedDataFile(
        os.path.join(data_directory_path, data_file.name),
        data_file.url,
        data_file.size,
        data_file.signed,
        data_file.compressed,
    )


def _to_data_file(prepared_data_file: PreparedDataFile):
    return DataFile(
        name=os.path.basename(prepared_data_file.path),
        url=prepared_data_file.url,
        size=prepared_data_file.size,
        signed=prepared_data_file.signed,
        compressed=prepared_data_file.compressed,
    )


def _warn_deprecated(name: str, replacement: str):
    warnings.warn(
        f"`crunch.downloader.{name}` is deprecated and will be removed in the next major version, use `crunch.download.{replacement}` instead.",
        DeprecationWarning,
        stacklevel=3,
    )


class _PrintHandler(logging.Handler):

    def __init__(self, print: Callable[[str], Any]):
        super().__init__()

        self._print = print

    def emit(self, record: logging.LogRecord):
        self._print(self.format(record))


def _to_logger(print: Callable[[str], Any]) -> logging.Logger:
    logger = logging.Logger("crunch.downloader")
    logger.addHandler(_PrintHandler(print))

    return logger
