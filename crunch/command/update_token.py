from typing import TYPE_CHECKING

from crunch.command.init import exchange_clone_token

if TYPE_CHECKING:
    from crunch.repository import Repository


def update_token(
    repository: "Repository",
    clone_token: str,
):
    project_access = exchange_clone_token(clone_token)
    project = project_access.project

    repository.write_project_token(project_access.project_token)
    repository.update_project(
        project_name=project.name,
        user_id=project.user_id,
    )

    print("update-token: updated")
