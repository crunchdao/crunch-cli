from .. import api, utils


def update_token(
    clone_token: str,
):
    client = api.Client.from_env()

    project_info = utils.read_project_info()

    project_token = client.project_tokens.upgrade(clone_token)

    plain = project_token.plain
    project = project_token.project

    project_info.project_name = project.name
    project_info.user_id = project.user_id
    utils.write_project_info(project_info)
    utils.write_token(plain)

    print("token updated")
