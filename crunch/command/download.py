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

    @property
    def has_size(self):
        return self.size != -1


def _get_data_urls(
    session: utils.CustomSession,
    round_number: str,
    data_directory: str,
    competition_name: str,
    push_token: str,
) -> typing.Tuple[int, typing.Tuple[str, str, str], typing.Tuple[DataFile, DataFile, DataFile, DataFile]]:
    data_release = session.get(f"/v2/competitions/{competition_name}/rounds/{round_number}/phases/submission/data-release", params={
        "pushToken": push_token
    }).json()

    embargo = data_release["embargo"]
    number_of_features = data_release["numberOfFeatures"]
    id_column_name = data_release["columnNames"]["id"]
    moon_column_name = data_release["columnNames"]["moon"]
    target_column_name = data_release["columnNames"]["target"]
    prediction_column_name = data_release["columnNames"]["prediction"]
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
    y_test = get_file("yTest", "y_test")

    return (
        embargo,
        number_of_features,
        (
            id_column_name,
            moon_column_name,
            target_column_name,
            prediction_column_name,
        ),
        (
            x_train,
            y_train,
            x_test,
            y_test
        )
    )


def _download(data_file: DataFile, force: bool):
    if data_file is None:
        return

    print(f"download {data_file.path} from {cut_url(data_file.url)}")

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
    round_number = "@current",
    force=False,
):
    project_info = utils.read_project_info()
    push_token = utils.read_token()

    os.makedirs(constants.DOT_DATA_DIRECTORY, exist_ok=True)

    (
        embargo,
        number_of_features,
        (
            id_column_name,
            moon_column_name,
            target_column_name,
            prediction_column_name,
        ),
        (
            x_train,
            y_train,
            x_test,
            y_test
        )
    ) = _get_data_urls(
        session,
        round_number,
        constants.DOT_DATA_DIRECTORY,
        project_info.competition_name,
        push_token
    )

    _download(x_train, force)
    _download(y_train, force)
    _download(x_test, force)
    _download(y_test, force)

    return (
        embargo,
        number_of_features,
        (
            id_column_name,
            moon_column_name,
            target_column_name,
            prediction_column_name,
        ),
        (
            x_train.path,
            y_train.path,
            x_test.path,
            y_test.path if y_test.has_size else None
        )
    )


def download_no_data_available():
    today = datetime.date.today()

    print("\n---")

    # competition lunch
    if today <= datetime.date(2023, 5, 16):
        print("The data will be released on May 16th, 2023, 05.00 PM CET")
    else:
        print("No data is available yet")
