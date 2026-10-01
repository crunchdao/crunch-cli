from typing import TYPE_CHECKING

from crunch.api import Client

if TYPE_CHECKING:
    from crunch.repository import Repository


def update_token(
    repository: "Repository",
    clone_token: str,
):
    client = Client.from_env()

    project_token = client.project_tokens.upgrade(clone_token)

    plain = project_token.plain
    project = project_token.project

    repository.update_project(
        project_name=project.name,
        user_id=project.user_id,
    )
    repository.write_push_token(plain)

    print("update-token: updated")
