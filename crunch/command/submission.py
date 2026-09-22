from typing import Any, List, Optional, Sequence

import click

from crunch.api import SubmissionNotFoundException
from crunch.command._common import get_project, reformat_datetime
from crunch.external.humanfriendly import format_size
from crunch.utils import ascii_table


def submission_list(
    limit: Optional[int],
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

    ascii_table(
        headers=["#", "Message", "Size", "Model", "Created At"],
        values=rows,
    )

    if reached_limit:
        print(f"display: only displaying the first {limit} runs, use `--limit <n>` to show more or `--all` to show all")


def submission_show(submission_number: int):
    submission = _get_submission(submission_number)

    model = submission.model

    print("Submission Details:")
    print(f"  Number: {submission.number}")
    print(f"  Message: {submission.message!r}")
    print(f"  Size: {format_size(submission.total_size)}")
    print(f"  Created At: {reformat_datetime(submission.created_at)}")

    main_file_path = submission.main_file_path
    model_directory_path = submission.model_directory_path

    print("  Files:")
    for file in submission.files:
        suffix = ""
        if file.name == main_file_path:
            suffix = " [main file]"

        print(f"    {file.name} ({format_size(file.size)}){suffix}")

    print("")
    print("Model Details:")
    if model is not None:
        print(f"  Size: {format_size(model.total_size)}")
        print(f"  Directory: {model_directory_path}")

        print("  Files:")
        for file in model.files:
            print(f"    {file.name} ({format_size(file.size)})")
    else:
        print("  (no model)")

    print("")
    print("Runtime Options:")

    rows: List[Sequence[Any]] = []
    for option in submission.runtime_options:
        definition = option.definition
        gpu = definition.specification.gpu

        rows.append(
            (
                definition.name,
                definition.display_name,
                f"{definition.specification.memory.size} GB",
                f"{definition.specification.cpu.core_count} cores",
                f"{gpu.model} ({gpu.driver})" if gpu.model is not None else "(none)",
                f"{option.status.name}",
            )
        )

    ascii_table(
        headers=["Name", "Display Name", "RAM", "CPU", "GPU", "Status"],
        values=rows
    )


def _get_submission(number: int):
    try:
        return get_project().submissions.get(number)
    except SubmissionNotFoundException:
        print(f"submission: not found: #{number}")
        raise click.Abort()
