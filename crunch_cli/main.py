import click

from . import utils
from . import command

session = None
debug = False


@click.group()
@click.option("--debug", "enable_debug", envvar="DEBUG", is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", envvar="API_BASE_URL", required=True, help="Set the API base url.")
@click.option("--web-base-url", envvar="WEB_BASE_URL", required=True, help="Set the Web base url.")
def cli(
    enable_debug: bool,
    api_base_url: str,
    web_base_url: str,
):
    global debug
    debug = enable_debug

    global session
    session = utils.CustomSession(
        web_base_url,
        api_base_url,
        debug
    )


@cli.command(help="Setup a workspace directory with the latest version of you code.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
@click.option("--version", "version_number", required=False, type=int, help="Version number to clone. (latest if not specified)")
@click.argument("project-name", required=True)
@click.argument("directory", default="{projectName}")
def setup(
    clone_token: str,
    version_number: str,
    project_name: str,
    directory: str,
):
    command.setup(
        session,
        clone_token=clone_token,
        version_number=version_number,
        project_name=project_name,
        directory=directory
    )


@cli.command(help="Send the new version of your code.")
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
def push(
    message: str
):
    command.push(
        session,
        message=message
    )


@cli.command(help="Test your code locally.")
@click.option("-m", "--main-file", default="main.py", show_default=True, help="Entrypoint of your code.")
def test(
    main_file: str
):
    command.test(
        main_file=main_file
    )


if __name__ == '__main__':
    cli()
