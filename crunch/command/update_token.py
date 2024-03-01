from .. import api, utils


def update_token(
    clone_token: str,
):
    client = api.Client.from_env()

    project_info = utils.read_project_info()

    project_token = client.project_tokens.upgrade(clone_token)

    plain = project_token.plain
    user_id = project_token.project.user_id

    project_info.user_id = user_id
    utils.write_project_info(project_info)
    utils.write_token(plain)

    print("token updated")
