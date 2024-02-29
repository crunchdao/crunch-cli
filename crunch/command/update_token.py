import click

from .. import api, utils


def update_token(
    client: api.Client,
    clone_token: str,
):
    project_info = utils.read_project_info()

    try:
        project_token = client.project_tokens.upgrade(clone_token)
    except api.InvalidProjectTokenException:
        print("your token seems to have expired or is invalid")
        print("---")
        print("please follow this link to copy and paste your new token:")
        print(client.format_web_url(f'/competitions/{project_info.competition_name}/submit'))
        print("")

        raise click.Abort()

    plain = project_token.plain
    user_id = project_token.project.id

    project_info.user_id = user_id
    utils.write_project_info(project_info)
    utils.write_token(plain)

    print("token updated")
