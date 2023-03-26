import click

from . import utils
from . import command

session = None
debug = False


@click.group()
@click.option("--debug", "enable_debug", envvar="DEBUG", is_flag=True)
@click.option("--api-base-url", envvar="API_BASE_URL", required=True)
def cli(
    enable_debug: bool,
    api_base_url: str,
):
    global debug
    debug = enable_debug

    global session
    session = utils.CustomSession(api_base_url, debug)


@cli.command()
@click.option("--token", "clone_token", required=True, help="clone token")
@click.option("--version", "version_number", required=False, type=int, help="version number to clone")
@click.argument("project_name", required=True)
@click.argument("directory", default="{projectName}")
def clone(
    clone_token: str,
    version_number: str,
    project_name: str,
    directory: str,
):
    command.clone(
        session,
        clone_token=clone_token,
        version_number=version_number,
        project_name=project_name,
        directory=directory
    )


@cli.command()
@click.option("-m", "--message", prompt=True, default="")
def push(
    message: str
):
    command.push(
        session,
        message=message
    )


@cli.command()
@click.option("-m", "--main-file", default="main.py")
def test(
    main_file: str
):
    command.test(
        main_file=main_file
    )


if __name__ == '__main__':
    cli()
