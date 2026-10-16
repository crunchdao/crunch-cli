from datetime import datetime, timezone
from typing import Optional

from crunch.repository import Repository


def get_project(repository: Repository):
    _, project = repository.create_client()

    return project.reload()


def reformat_datetime(
    input: Optional[datetime],
    *,
    default: Optional[str] = None,
):
    if input is None:
        return default

    return (
        input
        .replace(tzinfo=timezone.utc, microsecond=0)
        .astimezone(tz=None)
        .replace(tzinfo=None)
        .isoformat()
        .replace("T", " ")
    )
