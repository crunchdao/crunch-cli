import logging
import os

from crunch.api import RoundIdentifierType, RoundNotFoundException, SizeVariant
from crunch.command._common import get_cli_logger
from crunch.download import download_data_files
from crunch.repository import Repository


def download(
    repository: Repository,
    round_number: RoundIdentifierType = "@current",
    force: bool = False,
    size_variant: SizeVariant | None = None,
    *,
    logger: logging.Logger | None = None,
    verbose: bool = False,
):
    if logger is None:
        logger = get_cli_logger()

    _, project = repository.create_client()

    current_size_variant = repository.get_project().size_variant
    if size_variant is None:
        size_variant = current_size_variant

        changed_variant = True
    elif current_size_variant != size_variant:
        repository.update_project(size_variant=size_variant)
        logger.info(f"download: set default size variant: {size_variant.name.lower()}")

        changed_variant = True
    else:
        changed_variant = False

    competition = project.competition

    try:
        round = competition.rounds.get(round_number)
    except RoundNotFoundException as original:
        if round_number != "@current":
            raise

        if verbose:
            logger.info("download: current round not found, trying last")

        try:
            round = competition.rounds.last
        except RoundNotFoundException:
            raise original

    data_release = round.phases.get_submission().get_data_release(size_variant=size_variant)
    data_files = data_release.data_files
    assert data_files is not None, "the data release of a phase always has its data files"

    data_directory_path = os.path.relpath(repository.data_directory_path)
    os.makedirs(data_directory_path, exist_ok=True)

    logger.info(f"data: downloading the files of round #{round.number} into {data_directory_path}/")

    file_paths = download_data_files(
        data_directory_path,
        data_files,
        force=force,
        logger=logger,
        verbose=verbose,
        delete_other_uncompressed_markers=changed_variant,
    )

    return (
        data_directory_path,
        file_paths,
    )


def download_no_data_available():
    print("\n---")
    print("No data is available yet via the crunch-cli.")
    print("Read the documentation for more information.")
