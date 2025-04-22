import os
import typing

from .. import api, constants, container, downloader, utils


def _get_data_urls(
    round: api.Round,
    data_directory_path: str,
    size_variant: api.SizeVariant,
) -> typing.Tuple[
    int,
    int,
    typing.List[api.SplitKeyPythonType],
    container.Features,
    api.ColumnNames,
    typing.Dict[str, downloader.PreparedDataFile]
]:
    data_release = round.phases.get_submission().get_data_release(size_variant=size_variant)

    embargo = data_release.embargo
    number_of_features = data_release.number_of_features
    column_names = data_release.column_names
    data_files = data_release.data_files
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

    return (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        downloader.prepare_all(data_directory_path, data_files),
    )


def download(
    round_number: api.RoundIdentifierType = "@current",
    force=False,
    size_variant: typing.Optional[api.SizeVariant] = None,
):
    client, project = api.Client.from_project()

    project_info = client.project_info
    if size_variant is None:
        size_variant = project_info.size_variant
    elif project_info.size_variant != size_variant:
        project_info.size_variant = size_variant
        utils.write_project_info(project_info)
        print(f"project: set default size variant: {size_variant.name.lower()}")

    competition = project.competition

    try:
        round = competition.rounds.get(round_number)
    except api.RoundNotFoundException as original:
        if round_number != "@current":
            raise

        print(f"download: current round not found, trying last")

        try:
            round = competition.rounds.last
        except api.RoundNotFoundException:
            raise original

    data_directory_path = constants.DOT_DATA_DIRECTORY
    os.makedirs(data_directory_path, exist_ok=True)

    (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        prepared_data_files,
    ) = _get_data_urls(
        round,
        data_directory_path,
        size_variant,
    )

    file_paths = downloader.save_all(
        prepared_data_files,
        force,
    )

    return (
        embargo,
        number_of_features,
        split_keys,
        features,
        column_names,
        data_directory_path,
        file_paths,
    )


def download_no_data_available():
    print("\n---")
    print("No data is available yet via the crunch-cli.")
    print("Read the documentation for more information.")
