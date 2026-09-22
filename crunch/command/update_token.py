from crunch.api import Client
from crunch.utils import read_project_info, write_project_info, write_token


def update_token(
    clone_token: str,
):
    client = Client.from_env()

    project_info = read_project_info()

    project_token = client.project_tokens.upgrade(clone_token)

    project = project_token.project
    plain = project_token.plain
    assert plain is not None

    project_info.project_name = project.name
    project_info.user_id = project.user_id
    write_project_info(project_info)
    write_token(plain)

    print("token updated")
