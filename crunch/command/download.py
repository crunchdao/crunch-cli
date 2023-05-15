import os
import typing
import datetime
import dataclasses

import click
import requests
import tqdm

from .. import constants, utils


def cut_url(url: str):
    try:
        return url[:url.index("?")]
    except ValueError:
        return url


def get_extension(url: str):
    url = cut_url(url)

    if url.endswith(".parquet"):
        return "parquet"

    if url.endswith(".csv"):
        return "csv"

    print(f"unknown file extension: {url}")
    raise click.Abort()


@dataclasses.dataclass
class DataFile:

    url: str
    path: str
    size: int
    signed: bool


def get_data_urls(
    session: utils.CustomSession,
    data_directory: str,
    push_token: str,
) -> typing.Tuple[typing.Dict[str, str], str, str, str]:
    current_crunch = session.get("/v1/crunches/@current").json()
    data_release = session.get(f"/v1/crunches/{current_crunch['number']}/data-release", params={
        "pushToken": push_token
    }).json()

    embargo = data_release["embargo"]
    moon_column_name = data_release["moonColumnName"]
    data_files = data_release["dataFiles"]

    def get_file(key: str, file_name: str) -> DataFile:
        data_file = data_files[key]

        url = data_file["url"]
        path = os.path.join(
            data_directory,
            f"{file_name}.{get_extension(url)}"
        )

        size = data_file["size"]
        signed = data_file["signed"]

        return DataFile(url, path, size, signed)

    x_train = get_file("xTrain", "X_train")
    y_train = get_file("yTrain", "y_train")
    x_test = get_file("xTest", "X_test")

    return (
        embargo,
        moon_column_name,
        x_train,
        y_train,
        x_test
    )


def _download(data_file: DataFile, force: bool):
    print(f"download {data_file.path} from {cut_url(data_file.url)}")

    exists = os.path.exists(data_file.path)
    if not force and exists:
        stat = os.stat(data_file.path)
        if stat.st_size == data_file.size:
            print(f"already exists: file length match")
            return

    if not data_file.signed:
        print(f"signature missing: cannot download file without being authenticated")
        raise click.Abort()

    with requests.get(data_file.url, stream=True) as response:
        response.raise_for_status()

        file_length = response.headers.get("Content-Length", None)
        file_length = int(file_length) if not None else None

        with open(data_file.path, 'wb') as fd, tqdm.tqdm(total=file_length, unit='iB', unit_scale=True, leave=False) as progress:
            for chunk in response.iter_content(chunk_size=8192):
                progress.update(len(chunk))
                fd.write(chunk)


def download(
    session: utils.CustomSession,
    force=False,
):
    push_token = utils.read_token(raise_if_missing=False)

    os.makedirs(constants.DOT_DATA_DIRECTORY, exist_ok=True)

    (
        embargo,
        moon_column_name,
        x_train,
        y_train,
        x_test
    ) = get_data_urls(session, constants.DOT_DATA_DIRECTORY, push_token)

    _download(x_train, force)
    _download(y_train, force)
    _download(x_test, force)

    return (
        embargo,
        moon_column_name,
        x_train.path,
        y_train.path,
        x_test.path
    )


def download_no_data_available():
    today = datetime.date.today()

    print("\n---")

    # competition lunch
    if today <= datetime.date(2023, 5, 16):
        print("The data will be released on May 16th, 2023, 05.00 PM CET")
    else:
        print("No data is available yet")
