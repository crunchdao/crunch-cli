import click

from .. import api, utils


def update_token(
    session: utils.CustomSession,
    clone_token: str,
):
    project_info = utils.read_project_info()

    try:
        push_token = session.post(
            f"/v2/project-tokens/upgrade",
            json={
                "cloneToken": clone_token
            }
        ).json()
    except api.InvalidProjectTokenException:
        print("your token seems to have expired or is invalid")
        print("---")
        print("please follow this link to copy and paste your new token:")
        print(session.format_web_url(
            f'/competitions/{project_info.competition_name}/submit'
        ))
        print("")

        raise click.Abort()

    plain = push_token['plain']
    user_id = push_token['project']["userId"]

    project_info.user_id = user_id
    utils.write_project_info(project_info)
    utils.write_token(plain)

    print("token updated")
