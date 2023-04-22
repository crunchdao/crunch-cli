import click
import os

from . import utils, constants
from . import command

session = None
debug = False


@click.group()
@click.option("--debug", "enable_debug", envvar="CRUNCH_DEBUG", is_flag=True, help="Enable debug output.")
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


@cli.command(help="Setup a workspace directory with the latest submission of you code.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
@click.option("--submission", "submission_number", required=False, type=int, help="Submission number to clone. (latest if not specified)")
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.argument("project-name", required=True)
@click.argument("directory", default="{projectName}")
def setup(
    clone_token: str,
    submission_number: str,
    no_data: bool,
    project_name: str,
    directory: str,
    model_directory_path: str,
):
    directory = directory.replace("{projectName}", project_name)

    command.setup(
        session,
        clone_token=clone_token,
        submission_number=submission_number,
        project_name=project_name,
        directory=directory,
        model_directory=model_directory_path,
    )

    if not no_data:
        os.chdir(directory)
        command.download(session, force=True)

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")
    print(f" - To get inside your workspace directory, run: cd {directory}")
    print(f" - To see all of the available commands of the CrunchDAO CLI, run: crunch --help")


@cli.command(help="Send the new submission of your code.")
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
@click.option("-e", "--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
):
    utils.change_root()

    converted = False
    if not os.path.exists(main_file_path):
        print(f"missing {main_file_path}")

        file_name, _ = os.path.splitext(os.path.basename(main_file_path))
        dirpath = os.path.dirname(main_file_path)
        path_without_extension = os.path.join(dirpath, file_name)

        notebook_file_path = f"{path_without_extension}.ipynb"

        if not os.path.exists(notebook_file_path):
            raise click.Abort()

        main_file_path = constants.CONVERTED_MAIN_PY
        command.convert(notebook_file_path, main_file_path)
        converted = True

    try:
        submission = command.push(
            session,
            message=message,
            main_file_path=main_file_path,
            model_directory_path=model_directory_path,
        )

        command.push_summary(submission, session)
    finally:
        if converted:
            os.unlink(main_file_path)


@cli.command(help="Test your code locally.")
@click.option("-m", "--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop.")
@click.option("--train-frequency", default=1, show_default=True, help="Train interval.")
def test(
    main_file_path: str,
    model_directory_path: str,
    no_force_first_train: bool,
    train_frequency: int,
):
    utils.change_root()

    command.test(
        session,
        main_file_path=main_file_path,
        model_directory_path=model_directory_path,
        force_first_train=not no_force_first_train,
        train_frequency=train_frequency,
    )


@cli.command(help="Download the data locally.")
def download():
    utils.change_root()

    command.download(session)


@cli.command(help="Convert a notebook to a python script.")
@click.option("-o", "--override", is_flag=True, help="Force overwrite of the python file.")
@click.argument("notebook-file-path", required=True)
@click.argument("python-file-path", default="main.py")
def convert(
    override: bool,
    notebook_file_path: str,
    python_file_path: str,
):
    command.convert(
        notebook_file_path=notebook_file_path,
        python_file_path=python_file_path,
        override=override,
    )


if __name__ == '__main__':
    cli()
