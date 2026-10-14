from datetime import datetime, timezone
from typing import Optional

from crunch.api import Client


def get_project():
    _, project = Client.from_project()

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
