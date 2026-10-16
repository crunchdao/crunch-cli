from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import click

from crunch.api import NeverSubmittedException, SubmissionNotFoundException
from crunch.repository import Repository

if TYPE_CHECKING:
    from crunch.api import Project, SubmissionIdentifierType


def get_project(repository: Repository):
    _, project = repository.create_client()

    return project.reload()


def get_submission(
    project: "Project",
    identifier: "SubmissionIdentifierType",
    *,
    show_tips: bool = False,
):
    try:
        return project.submissions.get(identifier)
    except NeverSubmittedException:
        print(f"submission: you must submit at least once")

        if show_tips:
            print()
            print("tips:")
            print(f"  - Submit using `crunch push`.")

        raise click.Abort()
    except SubmissionNotFoundException:
        print(f"submission: not found: {identifier}")

        if show_tips:
            print()
            print("tips:")
            print(f"  - List already submitted submissions using `crunch submission list`.")

        raise click.Abort()


def reformat_datetime(
    input: Optional[datetime],
    *,
    default: Optional[str] = None,
):
    if input is None:
        return default

    if input.utcoffset() is None:
        input = input.replace(tzinfo=timezone.utc)

    return input.astimezone().replace(tzinfo=None, microsecond=0).isoformat().replace("T", " ")
