import os
import logging

import click

from . import command, constants, utils, api, library, tester

session = None
debug = False


@click.group()
@click.option("--debug", "enable_debug", envvar=constants.DEBUG_ENV_VAR, is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", envvar=constants.API_BASE_URL_ENV_VAR, default=constants.API_BASE_URL_DEFAULT, help="Set the API base url.")
@click.option("--web-base-url", envvar=constants.WEB_BASE_URL_ENV_VAR, default=constants.WEB_BASE_URL_DEFAULT, help="Set the Web base url.")
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
        debug,
    )


@cli.command(help="Setup a workspace directory with the latest submission of you code.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
@click.option("--submission", "submission_number", required=False, type=int, help="Submission number to clone. (latest if not specified)")
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--no-model", is_flag=True, help="Do not download the model of the cloned submission.")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.argument("competition-name", required=True)
@click.argument("directory", default="{competitionName}")
def setup(
    clone_token: str,
    submission_number: str,
    no_data: bool,
    no_model: bool,
    force: bool,
    competition_name: str,
    directory: str,
    model_directory_path: str,
):
    directory = directory\
        .replace("{competitionName}", competition_name)

    directory = os.path.normpath(directory)

    command.setup(
        session,
        clone_token=clone_token,
        submission_number=submission_number,
        competition_name=competition_name,
        directory=directory,
        model_directory=model_directory_path,
        force=force,
        no_model=no_model,
    )

    if not no_data:
        os.chdir(directory)

        try:
            command.download(session, force=True)
        except api.CurrentCrunchNotFoundException:
            command.download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")

    if directory != '.':
        print(
            f" - To get inside your workspace directory, run: cd {directory}")

    print(f" - To see all of the available commands of the CrunchDAO CLI, run: crunch --help")


@cli.command(help="Send the new submission of your code.")
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
@click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--export", "export_path", show_default=True, type=str, help="Copy the `.tar` to the specified file.")
@click.option("--no-pip-freeze", is_flag=True, help="Do not do a `pip freeze` to know preferred packages version.")
def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
    export_path: str,
    no_pip_freeze: bool,
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
        command.push(
            session,
            message=message,
            main_file_path=main_file_path,
            model_directory_path=model_directory_path,
            export_path=export_path,
            include_installed_packages_version=not no_pip_freeze
        )
    finally:
        if converted:
            os.unlink(main_file_path)


@cli.command(help="Test your code locally.")
@click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop.")
@click.option("--train-frequency", default=1, show_default=True, help="Train interval.")
@click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check.")
@click.option("--round-number", default="@current", help="Change round number to get the data from.")
@click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`.")
def test(
    main_file_path: str,
    model_directory_path: str,
    no_force_first_train: bool,
    train_frequency: int,
    skip_library_check: bool,
    round_number: str,
    has_gpu: str,
):
    utils.change_root()
    tester.install_logger()

    if not skip_library_check and os.path.exists(constants.REQUIREMENTS_TXT):
        library.scan(session, requirements_file=constants.REQUIREMENTS_TXT)
        logging.warn('')

    command.test(
        session,
        main_file_path=main_file_path,
        model_directory_path=model_directory_path,
        force_first_train=not no_force_first_train,
        train_frequency=train_frequency,
        round_number=round_number,
        has_gpu=has_gpu,
    )


@cli.command(help="Download the data locally.")
@click.option("--round-number", default="@current")
def download(
    round_number: str
):
    utils.change_root()

    try:
        command.download(
            session,
            round_number=round_number
        )
    except api.CurrentCrunchNotFoundException:
        command.download_no_data_available()


@cli.command(help="Convert a notebook to a python script.")
@click.option("--override", is_flag=True, help="Force overwrite of the python file.")
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
