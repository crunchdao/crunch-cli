import os
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Sequence, Set

import click

from crunch.api import EncryptedSubmissionException
from crunch.command._common import get_project, get_submission, reformat_datetime
from crunch.constants import DOT_CRUNCH_DIRECTORY, DOT_DATA_DIRECTORY, DOT_PREDICTION_DIRECTORY
from crunch.external.humanfriendly import format_size
from crunch.repository import Repository
from crunch.utils import ascii_table, download, to_unix_path

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType


class SubmissionIdentifierClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "identifier"

    def get_metavar(self, param: click.Parameter, ctx: Optional[click.Context] = None) -> Optional[str]:  # pyright: ignore[reportIncompatibleMethodOverride]
        # keep the argument name in usage, brackets there would read as optional
        if isinstance(param, click.Argument):
            return None

        return "[@last|<number>]"

    def convert(self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]) -> "SubmissionIdentifierType":
        if "@last" == value:
            return "@last"

        if isinstance(value, int) or value.isdigit():
            return int(value)

        self.fail(
            f"'{value}' is not a valid integer.",
            param,
            ctx
        )


def submission_list(
    repository: Repository,
    *,
    limit: Optional[int],
    show_tips: bool = False,
):
    submissions = get_project(repository).submissions.list()

    reached_limit = limit is not None and len(submissions) > limit

    rows: List[Sequence[Any]] = []
    for submission in reversed(submissions):
        model = submission.model

        rows.append(
            (
                submission.number,
                repr(submission.message),
                format_size(submission.total_size),
                format_size(model.total_size) if model is not None else "(none)",
                reformat_datetime(submission.created_at),
            )
        )

    if reached_limit:
        rows = rows[:limit]

    print("submissions:")
    ascii_table(
        headers=["#", "Message", "Size", "Model", "Created At"],
        values=rows,
    )

    if reached_limit:
        print()
        print(f"pagination: only displaying the first {limit} submissions, use `--limit <n>` to show more or `--all` to show all")

    if show_tips:
        print()
        print(f"tips:")
        print(f"  - To show a submission details, use `crunch submission show <number>`.")


def submission_show(
    repository: Repository,
    *,
    submission_identifier: "SubmissionIdentifierType",
    show_tips: bool = False,
):
    submission = get_submission(get_project(repository), submission_identifier, show_tips=show_tips)

    model = submission.model

    print("submission:")
    print(f"  number: {submission.number}")
    print(f"  message: {submission.message!r}")
    print(f"  size: {format_size(submission.total_size)}")
    print(f"  created at: {reformat_datetime(submission.created_at)}")

    main_file_path = submission.main_file_path
    model_directory_path = submission.model_directory_path

    print("  Files:")
    for file in submission.files:
        suffix = ""
        if file.name == main_file_path:
            suffix = " [main file]"

        print(f"    {file.name} ({format_size(file.size)}){suffix}")

    print()
    print("model:")
    if model is not None:
        print(f"  size: {format_size(model.total_size)}")
        print(f"  directory: {model_directory_path}")

        print("  Files:")
        for file in model.files:
            print(f"    {file.name} ({format_size(file.size)})")
    else:
        print("  (no model)")

    if show_tips:
        print()
        print(f"tips:")
        print(f"  - To create a run using this submission, use `crunch run create --submission {submission.number}`.")


def submission_checkout(
    repository: Repository,
    *,
    submission_identifier: "SubmissionIdentifierType",
    files: Optional[List[str]],
    show_tips: bool = False,
):
    requested_paths: Optional[Set[str]] = None
    if files is not None:
        requested_paths = set()

        for file in files:
            project_path = _to_project_path(repository, file)
            if project_path is None:
                print(f"checkout: {file}: outside of the project")
                raise click.Abort()

            requested_paths.add(project_path)

    submission = get_submission(get_project(repository), submission_identifier, show_tips=show_tips)

    try:
        urls = submission.project.clone(
            submission_number=submission.number,
            include_model=True,
        )
    except EncryptedSubmissionException:
        print(f"checkout: submission #{submission.number} is encrypted and cannot be checked out")
        raise click.Abort()

    if requested_paths is not None:
        selected_paths: Set[str] = set()
        unmatched_paths: List[str] = []

        for requested_path in requested_paths:
            matched_paths = _match_paths(urls.keys(), requested_path)
            if matched_paths:
                selected_paths.update(matched_paths)
            else:
                unmatched_paths.append(requested_path)

        if unmatched_paths:
            for unmatched_path in unmatched_paths:
                specific_message = _find_specific_message(unmatched_path)
                if specific_message is None:
                    specific_message = "not found in submission"

                print(f"checkout: {unmatched_path}: {specific_message}")

            if show_tips:
                print()
                print("tips:")
                print(f"  - To list the files of the submission, use `crunch submission show {submission.number}`.")

            raise click.Abort()

        urls = {
            path: url
            for path, url in urls.items()
            if path in selected_paths
        }

    for path, url in urls.items():
        download(url, path)

    print(f"checkout: {len(urls)} file(s) restored from submission #{submission.number}")

    if show_tips:
        print()
        print("tips:")
        print("  - Local files that are not part of the submission are left untouched.")


_SPECIFIC_MESSAGES = {
    DOT_DATA_DIRECTORY: "the data is never part of a submission, use `crunch download` instead",
    DOT_PREDICTION_DIRECTORY: "only for local use, cannot be checked out",
    DOT_CRUNCH_DIRECTORY: "only for local use, cannot be checked out",
}


def _find_specific_message(project_path: str) -> Optional[str]:
    for directory, message in _SPECIFIC_MESSAGES.items():
        if project_path.rstrip("/") == directory or project_path.startswith(directory + "/"):
            return message

    return None


def _match_paths(paths: Iterable[str], requested_path: str) -> List[str]:
    paths = list(paths)
    if requested_path in paths:
        return [requested_path]

    directory_prefix = requested_path.rstrip("/") + "/"
    if directory_prefix == "./":  # the project root
        return paths

    return [
        path
        for path in paths
        if path.startswith(directory_prefix)
    ]


def _to_project_path(repository: Repository, path: str) -> Optional[str]:
    try:
        relative_path = os.path.relpath(os.path.abspath(path), repository.root_directory_path)
    except ValueError:  # on windows, when the path is on another drive
        return None

    relative_path = to_unix_path(relative_path)
    if relative_path == ".." or relative_path.startswith("../"):
        return None

    return relative_path

