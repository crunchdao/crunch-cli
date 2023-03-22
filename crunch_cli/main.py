import click
import os
import tarfile
import io
import tempfile
import gitignorefile

from . import utils
from . import constants

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
    directory = directory.replace("{projectName}", project_name)

    if os.path.exists(directory):
        print(f"{directory}: already exists")
        raise click.Abort()

    push_token = session.post(f"/v1/projects/{project_name}/tokens", json={
        "type": "PERMANENT",
        "cloneToken": clone_token
    }).json()

    dot_crunchdao_path = os.path.join(
        directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    os.makedirs(dot_crunchdao_path)

    project_file_path = os.path.join(
        dot_crunchdao_path, constants.PROJECT_FILE)
    with open(project_file_path, "w") as fd:
        fd.write(project_name)

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(push_token['plain'])

    code_tar = io.BytesIO(
        session.get(f"/v1/projects/{project_name}/clone", params={
            "pushToken": push_token['plain'],
            "versionNumber": version_number,
        }).content
    )

    tar = tarfile.open(fileobj=code_tar)
    for member in tar.getmembers():
        path = os.path.join(directory, member.name)
        print(f"extract {path}")

        fileobj = tar.extractfile(member)
        with open(path, "wb") as fd:
            fd.write(fileobj.read())

    print("\n---")
    print(f"your project is available at: {directory}")
    print(f" - cd {directory}")
    print(f" - crunchdao-cli run")


@cli.command()
@click.option("-m", "--message", prompt=True, default="")
def push(
    message: str
):
    utils.change_root()

    project_name = utils.read_project_name()
    push_token = utils.read_token()

    matches = gitignorefile.Cache()

    with tempfile.NamedTemporaryFile(prefix="version-", suffix=".tar") as tmp:
        with tarfile.open(fileobj=tmp, mode="w") as tar:
            for root, dirs, files in os.walk(".", topdown=False):
                if root.startswith("./"):
                    root = root[2:]
                elif root == ".":
                    root = ""

                for file in files:
                    file = os.path.join(root, file)

                    ignored = False
                    for ignore in constants.IGNORED_FILES:
                        if ignore in file:
                            ignored = True
                            break

                    if ignored or matches(file):
                        continue

                    print(f"compress {file}")
                    tar.add(file)

        with open(tmp.name, "rb") as fd:
            version = session.post(
                f"/v1/projects/{project_name}/versions",
                data={
                    "message": message,
                    "pushToken": push_token,
                },
                files={
                    "tarFile": ('code.tar', fd, "application/x-tar")
                }
            ).json()

    print("\n---")
    print(f"version #{version['number']} uploaded!")


if __name__ == '__main__':
    cli()
