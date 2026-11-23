import logging
import os
import re
import shutil
import subprocess
import time
import zipfile
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import click

from crunch.constants import MACOS_HIDDEN_FILES
from crunch.external.humanfriendly import format_size

if TYPE_CHECKING:
    import requests

    from crunch.api import DataFile, DataFiles

__all__ = [
    "cut_url",
    "download_file",
    "download_files",
    "download_data_files",
]


def cut_url(url: str):
    """Without the query, which holds the signature, and with `://` replaced by `:` so that notebooks do not make it a link."""

    try:
        url = url[:url.index("?")]
    except ValueError:
        pass

    return url.replace("://", ":", 1)


def download_file(
    url: str,
    path: str,
    *,
    logger: logging.Logger,
    verbose: bool = False,
    progress_bar: bool = True,
    max_retry: int = 10,
    session: "requests.Session | None" = None,
):
    import requests
    from tqdm.auto import tqdm

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    if session is None:
        session = requests.Session()

    # With an encoded response, like GitHub's gzip, the length and the range count the encoded bytes while the progress counts the decoded ones
    session.headers["Accept-Encoding"] = "identity"

    response = session.get(url, stream=True)

    try:
        _safely_raise_for_status(response)
    except BaseException:
        response.close()

        if verbose:
            logger.info(f"{path}: download from {cut_url(url)}")

        raise

    file_length = response.headers.get("Content-Length")
    file_length = int(file_length) if file_length is not None else None

    if verbose:
        logger.info(f"{path}: download from {cut_url(url)} ({format_size(file_length) if file_length is not None else 'unknown size'})")

    if response.headers.get("Accept-Ranges") != "bytes":
        max_retry = 0

    total_read = 0
    file_name = os.path.basename(path)

    with TemporaryDirectory(
        prefix=f"{file_name}.",
        dir=os.path.dirname(path) or ".",
    ) as temporary_directory_path:
        temporary_file_path = os.path.join(temporary_directory_path, file_name)

        with open(temporary_file_path, "wb") as fd:
            for retry in range(max_retry + 1):
                last = retry == max_retry

                try:
                    response = response or session.get(
                        url,
                        stream=True,
                        headers={
                            # Already for a retry.
                            "Range": f"bytes={total_read}-",
                        },
                        timeout=30,
                    )

                    _safely_raise_for_status(response)

                    with tqdm(
                        initial=total_read,
                        total=file_length,
                        unit="B",
                        unit_scale=True,
                        leave=False,
                        disable=None if progress_bar else True,
                    ) as progress:
                        for chunk in response.iter_content(chunk_size=1024 * 16):
                            chunk_size = len(chunk)
                            total_read += chunk_size

                            progress.update(chunk_size)
                            fd.write(chunk)

                    break
                except requests.exceptions.ConnectionError as error:
                    if last:
                        raise

                    logger.warning(f"{path}: retrying {retry + 1}/{max_retry} at {total_read} bytes because of {error.__class__.__name__}: {str(error) or '(no message)'}")
                    time.sleep(1)
                finally:
                    if response is not None:
                        response.close()

                    response = None

        if os.path.exists(path):
            os.unlink(path)

        shutil.move(temporary_file_path, path)


def download_files(
    urls: dict[str, str],
    *,
    logger: logging.Logger,
    verbose: bool = False,
    progress_bar: bool = True,
):
    """Downloads each url of `urls`, which are by path."""

    for path, url in urls.items():
        download_file(
            url,
            path,
            logger=logger,
            verbose=verbose,
            progress_bar=progress_bar,
        )


def download_data_files(
    data_directory_path: str,
    data_files: "DataFiles",
    *,
    force: bool,
    logger: logging.Logger,
    verbose: bool = False,
    progress_bar: bool = True,
    delete_other_uncompressed_markers: bool = False,
) -> dict[str, str]:
    """
    Downloads the data files that are not up to date, and uncompresses the compressed ones.
    Returns the paths of the files given by the server, by key.
    """

    paths = {
        key: os.path.join(data_directory_path, data_file.name)
        for key, data_file in data_files.items()
    }

    if delete_other_uncompressed_markers:
        _delete_other_uncompressed_markers(
            data_directory_path,
            expected_marker_paths={
                _to_uncompressed_marker_path(path)
                for path in paths.values()
            },
        )

    downloaded_count = 0
    for key, data_file in data_files.items():
        downloaded = _download_data_file(
            data_file,
            paths[key],
            force=force,
            logger=logger,
            verbose=verbose,
            progress_bar=progress_bar,
        )

        if downloaded:
            downloaded_count += 1

    available_paths = {
        key: path
        for key, path in paths.items()
        if data_files[key].size != -1
    }

    up_to_date_count = len(available_paths) - downloaded_count

    if downloaded_count and up_to_date_count:
        summary = f"{downloaded_count} file(s) downloaded, {up_to_date_count} already up to date"
    elif up_to_date_count:
        summary = f"{up_to_date_count} file(s) already up to date"
    else:
        summary = f"{downloaded_count} file(s) downloaded"

    logger.info(f"{data_directory_path}: {summary}")

    return available_paths


def _download_data_file(
    data_file: "DataFile",
    path: str,
    *,
    force: bool,
    logger: logging.Logger,
    verbose: bool,
    progress_bar: bool,
) -> bool:
    if data_file.size == -1:
        if verbose:
            logger.info(f"{path}: skip, not given by the server")

        return False

    marker_path = _to_uncompressed_marker_path(path)

    downloaded = force or _read_size(path, marker_path) != data_file.size
    if downloaded:
        if not data_file.signed:
            logger.error(f"{path}: signature missing, cannot download the file without being authenticated")
            raise click.Abort()

        download_file(
            data_file.url,
            path,
            logger=logger,
            verbose=verbose,
            progress_bar=progress_bar,
        )

    elif verbose:
        logger.info(f"{path}: already up to date")

    if data_file.compressed and (downloaded or not os.path.exists(marker_path)):
        if os.path.exists(marker_path):
            os.unlink(marker_path)

        if verbose:
            logger.info(f"{path}: uncompress")

        _uncompress_in_place(path, marker_path, data_file.size)

    return downloaded


def _to_uncompressed_marker_path(path: str):
    return os.path.join(
        os.path.dirname(path),
        f".{os.path.basename(path)}.uncompressed"
    )


def _delete_other_uncompressed_markers(
    data_directory_path: str,
    *,
    expected_marker_paths: set[str],
):
    for root, _, files in os.walk(data_directory_path, topdown=False):
        for file in files:
            if not (file.startswith(".") and file.endswith(".uncompressed")):
                continue

            path = os.path.join(root, file)
            if path not in expected_marker_paths:
                os.unlink(path)


def _read_size(
    file_path: str,
    marker_file_path: str,
):
    try:
        with open(marker_file_path, "r") as fd:
            content = fd.read()

        return int(content)
    except (FileNotFoundError, ValueError):
        pass

    try:
        return os.stat(file_path).st_size
    except FileNotFoundError:
        pass

    return None


def _uncompress_in_place(
    path: str,
    marker_path: str,
    size: int,
):
    parent_directory_path = os.path.dirname(path)

    with TemporaryDirectory(
        prefix=f"{os.path.basename(path)}.",
        dir=parent_directory_path,
    ) as temporary_directory_path:
        _uncompress(path, temporary_directory_path)

        for name in os.listdir(temporary_directory_path):
            if name in MACOS_HIDDEN_FILES:
                continue

            source_path = os.path.join(temporary_directory_path, name)
            destination_path = os.path.join(parent_directory_path, name)

            if os.path.exists(destination_path):
                if os.path.isdir(destination_path):
                    shutil.rmtree(destination_path)
                else:
                    os.unlink(destination_path)

            shutil.move(source_path, parent_directory_path)

    with open(marker_path, "w") as fd:
        # TODO Should include name of unzipped files as well for later cleanup
        fd.write(str(size))

    os.unlink(path)


def _uncompress(
    zip_file_path: str,
    output_directory_path: str,
):
    unzip = shutil.which("unzip")

    if unzip:
        return_code = subprocess.call([
            unzip,
            "-q",
            "-d", output_directory_path,
            zip_file_path
        ])

        if return_code != 0:
            raise zipfile.BadZipFile(f"unzip failed with code: {return_code}")
    else:
        with zipfile.ZipFile(zip_file_path, "r") as zipfd:
            zipfd.extractall(output_directory_path)


# copied from requests.models.Response.raise_for_status
def _safely_raise_for_status(self: "requests.Response"):
    """Raises :class:`HTTPError`, if one occurred."""

    import requests

    http_error_msg = ""
    if isinstance(self.reason, bytes):
        # We attempt to decode utf-8 first because some servers
        # choose to localize their reason strings. If the string
        # isn't utf-8, we fall back to iso-8859-1 for all other
        # encodings. (See PR #3538)
        try:
            reason = self.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = self.reason.decode("iso-8859-1")
    else:
        reason = self.reason

    url = self.url
    url = re.sub(r"(X-(Amz|Goog)-[A-Za-z]+=)[^&]*", r"\1(hidden)", url)

    if 400 <= self.status_code < 500:
        http_error_msg = (
            f"{self.status_code} Client Error: {reason} for url: {url}"
        )

    elif 500 <= self.status_code < 600:
        http_error_msg = (
            f"{self.status_code} Server Error: {reason} for url: {url}"
        )

    if http_error_msg:
        raise requests.HTTPError(http_error_msg, response=self)
