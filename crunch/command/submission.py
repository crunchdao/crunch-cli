from typing import TYPE_CHECKING, Any, List, Optional, Sequence

import click

from crunch.api import SubmissionNotFoundException
from crunch.command._common import get_project, reformat_datetime
from crunch.external.humanfriendly import format_size
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType


class SubmissionIdentifierClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "identifier"

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
    *,
    limit: Optional[int],
    show_tips: bool = False,
):
    submissions = get_project().submissions.list()

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
    *,
    submission_identifier: "SubmissionIdentifierType",
    show_tips: bool = False,
):
    submission = _get_submission(submission_identifier)

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


def _get_submission(identifier: "SubmissionIdentifierType"):
    try:
        return get_project().submissions.get(identifier)
    except SubmissionNotFoundException:
        print(f"submission: not found: #{identifier}")
        raise click.Abort()
