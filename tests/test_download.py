import io
import logging
import os
import tempfile
import threading
import unittest
import zipfile
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import requests

from crunch.api import DataFile
from crunch import utils
from crunch.download import cut_url, download_data_files, download_file, download_files
from crunch.downloader import prepare_all, save_all

_LOGGER_NAME = "test_download"


class CutUrlTest(unittest.TestCase):

    def test_regular(self):
        self.assertEqual("http:google.com/search", cut_url("http://google.com/search"))
        self.assertEqual("https:google.com/search", cut_url("https://google.com/search"))

    def test_remove_query(self):
        self.assertEqual("http:google.com/search", cut_url("http://google.com/search?q=hello"))
        self.assertEqual("https:google.com/search", cut_url("https://google.com/search?q=hello"))

    def test_keep_double(self):
        self.assertEqual("http:google.com//search//a", cut_url("http://google.com//search//a?q=hello"))
        self.assertEqual("https:google.com//search//a", cut_url("https://google.com//search//a?q=hello"))


class DownloadFileTest(unittest.TestCase):

    def test_signature_is_hidden(self):
        with self.assertRaises(requests.HTTPError) as context:
            download_file(
                "https://crunchdao--competition--production.s3.amazonaws.com/file?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260823T205142Z&X-Amz-SignedHeaders=host&X-Amz-Credential=AAAA&X-Amz-Expires=7200&X-Amz-Signature=BBBB",
                "/dev/null",
                logger=logging.getLogger(_LOGGER_NAME),
            )

        self.assertEqual("403 Client Error: Forbidden for url: https://crunchdao--competition--production.s3.amazonaws.com/file?X-Amz-Algorithm=(hidden)&X-Amz-Date=(hidden)&X-Amz-SignedHeaders=(hidden)&X-Amz-Credential=(hidden)&X-Amz-Expires=(hidden)&X-Amz-Signature=(hidden)", str(context.exception))


class DownloadDataFilesTest(unittest.TestCase):

    def setUp(self):
        self._served_directory = tempfile.TemporaryDirectory()
        self._data_directory = tempfile.TemporaryDirectory()

        with open(os.path.join(self._served_directory.name, "X_train.parquet"), "wb") as fd:
            fd.write(b"x" * 10)

        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zip_file:
            zip_file.writestr("y_train.parquet", b"y" * 5)

        self._archive_size = len(archive.getvalue())
        with open(os.path.join(self._served_directory.name, "y_train.zip"), "wb") as fd:
            fd.write(archive.getvalue())

        handler = partial(_QuietRequestHandler, directory=self._served_directory.name)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

        base_url = f"http://127.0.0.1:{self._server.server_address[1]}/competitions/structural-break"
        self._data_files = {
            "X_train": DataFile(name="X_train.parquet", url=f"{base_url}/X_train.parquet", size=10, signed=True, compressed=False),
            "y_train": DataFile(name="y_train.zip", url=f"{base_url}/y_train.zip", size=self._archive_size, signed=True, compressed=True),
            "X_test": DataFile(name="X_test.parquet", url=f"{base_url}/X_test.parquet", size=-1, signed=True, compressed=False),
        }

    def tearDown(self):
        self._server.shutdown()
        self._server.server_close()
        self._served_directory.cleanup()
        self._data_directory.cleanup()

    def _download(self, *, verbose: bool = False, force: bool = False):
        logger = logging.getLogger(_LOGGER_NAME)

        with self.assertLogs(logger) as logs:
            paths = download_data_files(
                self._data_directory.name,
                self._data_files,
                force=force,
                logger=logger,
                verbose=verbose,
                progress_bar=False,
            )

        return paths, [record.getMessage().replace(self._data_directory.name, "data") for record in logs.records]

    def test_quiet_by_default(self):
        paths, messages = self._download()

        self.assertEqual(["data: 2 file(s) downloaded"], messages)
        self.assertEqual({"X_train", "y_train"}, set(paths.keys()))

        with open(os.path.join(self._data_directory.name, "y_train.parquet"), "rb") as fd:
            self.assertEqual(b"y" * 5, fd.read())

        self.assertFalse(os.path.exists(paths["y_train"]))

    def test_already_up_to_date(self):
        self._download()
        _, messages = self._download()

        self.assertEqual(["data: 2 file(s) already up to date"], messages)

    def test_partially_up_to_date(self):
        paths, _ = self._download()
        os.unlink(paths["X_train"])

        _, messages = self._download()

        self.assertEqual(["data: 1 file(s) downloaded, 1 already up to date"], messages)

    def test_force(self):
        self._download()
        _, messages = self._download(force=True)

        self.assertEqual(["data: 2 file(s) downloaded"], messages)

    def test_verbose(self):
        _, messages = self._download(verbose=True)

        self.assertEqual(
            [
                f"{os.path.join('data', 'X_train.parquet')}: download from http:127.0.0.1:{self._server.server_address[1]}/competitions/structural-break/X_train.parquet (10 bytes)",
                f"{os.path.join('data', 'y_train.zip')}: download from http:127.0.0.1:{self._server.server_address[1]}/competitions/structural-break/y_train.zip ({self._archive_size} bytes)",
                f"{os.path.join('data', 'y_train.zip')}: uncompress",
                f"{os.path.join('data', 'X_test.parquet')}: skip, not given by the server",
                "data: 2 file(s) downloaded",
            ],
            messages,
        )


    def test_download_files(self):
        path = os.path.join(self._data_directory.name, "nested", "X_train.parquet")

        download_files(
            {path: self._data_files["X_train"].url},
            logger=logging.getLogger(_LOGGER_NAME),
            progress_bar=False,
        )

        with open(path, "rb") as fd:
            self.assertEqual(b"x" * 10, fd.read())


    def test_deprecated_save_all(self):
        printed: list[str] = []

        with self.assertWarns(DeprecationWarning):
            prepared_data_files = prepare_all(self._data_directory.name, self._data_files)

        with self.assertWarns(DeprecationWarning):
            paths = save_all(prepared_data_files, False, printed.append, progress_bar=False)

        self.assertEqual({"X_train", "y_train"}, set(paths.keys()))
        self.assertEqual(4, len(printed))

        with open(os.path.join(self._data_directory.name, "y_train.parquet"), "rb") as fd:
            self.assertEqual(b"y" * 5, fd.read())

    def test_deprecated_utils_download(self):
        path = os.path.join(self._data_directory.name, "X_train.parquet")
        printed: list[str] = []

        with self.assertWarns(DeprecationWarning):
            utils.download(self._data_files["X_train"].url, path, print=printed.append, progress_bar=False)

        self.assertEqual(1, len(printed))

        with open(path, "rb") as fd:
            self.assertEqual(b"x" * 10, fd.read())


class _QuietRequestHandler(SimpleHTTPRequestHandler):

    def translate_path(self, path: str) -> str:
        return super().translate_path("/" + path.rsplit("/", 1)[-1])

    def log_message(self, format: str, *args: object) -> None:
        pass
