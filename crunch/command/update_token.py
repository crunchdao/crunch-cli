from typing import TYPE_CHECKING

from crunch.api import Client
from crunch.repository import Authentication

if TYPE_CHECKING:
    from crunch.repository import Repository


def update_token(
    repository: "Repository",
    clone_token: str,
):
    client = Client.from_env()

    project_token = client.project_tokens.upgrade(clone_token)

    project = project_token.project
    plain = project_token.plain
    assert plain is not None

    repository.write_project_token(plain)
    repository.update_project(
        project_name=project.name,
        user_id=project.user_id,
        authentication=Authentication.PROJECT_TOKEN,
    )

    print("update-token: updated")
