from datetime import datetime, timezone
from typing import Optional

from crunch.api import Client
from crunch.utils import read_project_info


def get_project():
    client = Client.from_env()
    project_info = read_project_info()

    return (
        client
        .competitions.get(project_info.competition_name)
        .projects.get(project_info.user_id, project_info.project_name)
    )


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
