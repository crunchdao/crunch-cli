import json
import os
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Sequence

import click

from crunch.api import CompetitionFormat, CompetitionMode, CompetitionStatus, PhaseType, RoundIdentifierType
from crunch.command.init import API_KEY_CLONE_TOKEN, format_directory
from crunch.command.setup import SetupSubmissionNumberClickType, SetupSubmissionNumberType
from crunch.command.submission import SubmissionIdentifierClickType
from crunch.command.whitelist import GpuRequirementClickType, ProgrammingLanguageClickType
from crunch.constants import REQUIREMENTS_TXT
from crunch.notebook import NotebookSecretNotAccessibleError
from crunch.repository import Repository, RepositoryError
from crunch.unstructured.cli import organize_test_group

from . import __version__, api, constants, store

if TYPE_CHECKING:
    from crunch_convert import RequirementLanguage

    from crunch.api import GpuRequirement, SubmissionIdentifierType


DIRECTORY_DEFAULT_FORMAT = "{competitionName}-{projectName}"

DATA_SIZE_VARIANTS = [
    api.SizeVariant.DEFAULT.name.lower(),
    api.SizeVariant.SMALL.name.lower(),
    api.SizeVariant.LARGE.name.lower(),
]


def _echo_version():
    click.echo(f"{__version__.__title__}, version {__version__.__version__}")


def _open_repository() -> Repository:
    repository = Repository.open()

    os.chdir(repository.root_directory_path)
    return repository


_COMPETITION_NAME_META_KEY = "crunch.competition_name"


def _remember_competition_name(competition_name: str) -> None:
    click.get_current_context().meta[_COMPETITION_NAME_META_KEY] = competition_name


_token_option = click.option("--token", "clone_token", required=True, help=f"Clone token to use, or `{API_KEY_CLONE_TOKEN}` to authenticate with the CRUNCH_API_KEY secret of the notebook platform (Kaggle, Colab, ...).")


class _CliGroup(click.Group):

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except RepositoryError as error:
            print(f"project: {error}", file=sys.stderr)
            raise click.Abort()
        except NotebookSecretNotAccessibleError as error:
            print(f"secret: {error}", file=sys.stderr)
            print(f"secret: if the environment was originally created via a clone-token, the file might be missing: run `crunch update-token` to refresh it", file=sys.stderr)
            raise click.Abort()
        except api.ApiException as error:
            print("\n---")
            error.print_helper(
                competition_name=ctx.meta.get(_COMPETITION_NAME_META_KEY),
            )
            ctx.exit(1)


@click.group(cls=_CliGroup)
@click.version_option(__version__.__version__, package_name="__version__.__title__")
@click.option("--debug", is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", help="Set the API base url.")
@click.option("--web-base-url", help="Set the Web base url.")
@click.option("--competitions", "competitions_source", help="Set the Competitions repository location.")
@click.option("--environment", "--env", "environment_name", help="Connect to another environment, overwrites the base urls.")
@click.option("--hide-tips", "hide_tips", is_flag=True, help="Hide tips.")
def cli(
    debug: bool,
    api_base_url: str | None,
    web_base_url: str | None,
    competitions_source: str | None,
    environment_name: str | None,
    hide_tips: bool,
):
    constants.RUN_VIA_CLI = True

    store.override(
        debug=True if debug else None,
        environment=environment_name,
        api_base_url=api_base_url,
        web_base_url=web_base_url,
        competitions_source=competitions_source,
        show_tips=False if hide_tips else None,
    )


@cli.command(help="Test if the server is online.")
def ping():
    client = api.Client.from_env()

    try:
        client.competitions.get(1)
        print("server is up!")
    except BaseException as exception:
        print(f"server is down? ({exception})")


@cli.command(help="Initialize an empty workspace directory.")
@_token_option
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.argument("competition-name", required=True)
@click.argument("project-name", required=True)
@click.argument("directory", default=DIRECTORY_DEFAULT_FORMAT)
def init(
    clone_token: str,
    no_data: bool,
    force: bool,
    competition_name: str,
    project_name: str,
    directory: str,
    model_directory_path: str,
):
    from crunch.command.download import download, download_no_data_available
    from crunch.command.init import init

    _remember_competition_name(competition_name)

    directory = format_directory(directory, competition_name, project_name)

    try:
        repository = init(
            clone_token=clone_token,
            competition_name=competition_name,
            project_name=project_name,
            directory=directory,
            model_directory=model_directory_path,
            force=force,
        )

        if not no_data:
            download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly initialized.")


@cli.command(help="Setup a workspace directory.")
@_token_option
@click.option("--submission", "submission_number", required=False, type=SetupSubmissionNumberClickType(), default="latest", help='Submission number to clone. ("latest" if not specified, "scratch" to disable)')
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--no-model", is_flag=True, help="Do not download the model of the cloned submission.")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--size", "data_size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), default=DATA_SIZE_VARIANTS[0], help="Use another data variant.")
@click.argument("competition-name", required=True)
@click.argument("project-name", required=True)
@click.argument("directory", default=DIRECTORY_DEFAULT_FORMAT)
def setup(
    clone_token: str,
    submission_number: SetupSubmissionNumberType,
    no_data: bool,
    no_model: bool,
    force: bool,
    competition_name: str,
    project_name: str,
    directory: str,
    model_directory_path: str,
    data_size_variant_raw: str,
):
    from crunch.command.download import download, download_no_data_available
    from crunch.command.setup import setup

    _remember_competition_name(competition_name)

    _echo_version()

    directory = format_directory(directory, competition_name, project_name)

    data_size_variant = api.SizeVariant[data_size_variant_raw.upper()]

    try:
        repository = setup(
            clone_token=clone_token,
            competition_name=competition_name,
            project_name=project_name,
            submission_number=submission_number,
            directory=directory,
            model_directory=model_directory_path,
            force=force,
            no_model=no_model,
            data_size_variant=data_size_variant,
        )

        if not no_data:
            download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")

    if directory != ".":
        print(f" - To get inside your workspace directory, run: cd {directory}")

    print(f" - To see all of the available commands of the Crunch CLI, run: crunch --help")


@cli.command(help="Setup a notebook workspace.")
@click.option("--submission", "submission_number", required=False, type=SetupSubmissionNumberClickType(), default="latest", help='Submission number to clone. ("latest" if not specified, "scratch" to disable)')
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--no-model", is_flag=True, help="Do not download the model of the cloned submission.")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--size", "data_size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), default=DATA_SIZE_VARIANTS[0], help="Use another data variant.")
@click.argument("competition-name", required=True)
@click.argument("clone-token", required=True)
@click.argument("project-name", required=False)
def setup_notebook(
    submission_number: SetupSubmissionNumberType,
    no_data: bool,
    no_model: bool,
    model_directory_path: str,
    data_size_variant_raw: str,
    competition_name: str,
    clone_token: str,
    project_name: str | None,
):
    from crunch.command.download import download, download_no_data_available
    from crunch.command.setup import setup

    _remember_competition_name(competition_name)

    _echo_version()

    data_size_variant = api.SizeVariant[data_size_variant_raw.upper()]

    try:
        repository = setup(
            clone_token=clone_token,
            competition_name=competition_name,
            project_name=project_name,
            submission_number=submission_number,
            directory=os.getcwd(),
            model_directory=model_directory_path,
            force=True,
            no_model=no_model,
            data_size_variant=data_size_variant,
        )

        if not no_data:
            download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")
    print(f"1. Load the Crunch Toolings: `crunch_tools = crunch.load_notebook()`")
    print(f"2. Execute the cells with your code")
    print(f"3. Run a test: `crunch_tools.test()`")
    print(f"4. Download and submit your code to the platform!")


@contextmanager
def convert_if_necessary(
    main_file_path: str
):
    from crunch.command.convert import convert

    converted = False
    if not os.path.exists(main_file_path):
        print(f"missing {main_file_path}")

        file_name, _ = os.path.splitext(os.path.basename(main_file_path))
        dirpath = os.path.dirname(main_file_path)
        path_without_extension = os.path.join(dirpath, file_name)

        notebook_file_path = f"{path_without_extension}.ipynb"
        if not os.path.exists(notebook_file_path):
            raise click.Abort()

        convert(
            notebook_file_path,
            main_file_path,
            write_requirements=not os.path.exists(REQUIREMENTS_TXT),
        )

        converted = True
    try:
        yield
    finally:
        if converted:
            os.unlink(main_file_path)


@cli.command(help="Send the new submission of your code.")
@click.option("-m", "--message", prompt=True, default="", help=f"Specify the change of your code. (like a commit message, limited to {constants.SUBMISSION_MESSAGE_LENGTH} characters)")
@click.option("--main-file", "main_file_path", default=constants.DEFAULT_MAIN_FILE_PATH, show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--no-pip-freeze", is_flag=True, help="Do not do a `pip freeze` to know preferred packages version.")
@click.option("--dry", is_flag=True, help="Prepare file but do not really create the submission.")
def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
    no_pip_freeze: bool,
    dry: bool,
):
    from crunch.command.push import push

    repository = _open_repository()

    with convert_if_necessary(main_file_path):
        push(
            repository=repository,
            message=message,
            main_file_path=main_file_path,
            model_directory_relative_path=model_directory_path,
            include_installed_packages_version=not no_pip_freeze,
            no_afterword=False,
            dry=dry,
        )


@cli.command(help="Download the data locally.")
@click.option("--round-number", default="@current")
@click.option("--force", is_flag=True, help="Force the download of the data.")
@click.option("--size-variant", "size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), required=False, help="Use alternative version of the data.")
def download(
    round_number: "RoundIdentifierType",
    force: bool,
    size_variant_raw: str | None,
):
    from crunch.command.download import download, download_no_data_available

    repository = _open_repository()

    size_variant = (
        api.SizeVariant[size_variant_raw.upper()]
        if size_variant_raw is not None
        else None
    )

    try:
        download(
            repository,
            round_number,
            force,
            size_variant,
        )
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        download_no_data_available()


@cli.command(help="Convert a notebook to a python script.")
@click.option("--overwrite", is_flag=True, help="Force overwrite of the python file.")
@click.option("--requirements", is_flag=True, help="Also export the `requirements.txt` file.")
@click.option("--embedded-files", is_flag=True, help="Also export the embedded files.")
@click.option("--no-freeze", is_flag=True, help="Don't freeze the requirements with locally installed versions.")
@click.option("--verbose", is_flag=True, help="Enable verbose output.")
@click.argument("notebook-file-path", required=True)
@click.argument("python-file-path", default="main.py")
def convert(
    overwrite: bool,
    requirements: bool,
    embedded_files: bool,
    no_freeze: bool,
    verbose: bool,
    notebook_file_path: str,
    python_file_path: str,
):
    from crunch.command.convert import convert

    convert(
        notebook_file_path=notebook_file_path,
        python_file_path=python_file_path,
        overwrite=overwrite,
        write_requirements=requirements,
        write_embedded_files=embedded_files,
        no_freeze=no_freeze,
        verbose=verbose,
    )


@cli.command(help="Update a project token.")
@click.argument("clone-token", required=False)
def update_token(
    clone_token: str,
):
    from crunch.command.update_token import update_token

    if not clone_token:
        clone_token = click.prompt("Clone Token", hide_input=True)

    repository = _open_repository()

    update_token(
        repository,
        clone_token=clone_token
    )


@cli.command(help="Show information on the competition.")
def status():
    from crunch.command.status import status

    repository = _open_repository()

    status(
        repository,
    )


@cli.command(help="Show the project's rank on the leaderboard.")
def leaderboard():
    from crunch.command.leaderboard import leaderboard

    repository = _open_repository()

    leaderboard(
        repository,
    )


@cli.command(help="Show the project's quota.")
def quota():
    from crunch.command.quota import quota

    repository = _open_repository()

    quota(
        repository,
    )


@cli.group(name="competition", help="Manage competitions.")
def competition_group():
    pass


@competition_group.command(name="list", help="List available competitions.")
@click.option("--format", "format_raw", required=False, type=click.Choice([x.name for x in CompetitionFormat], case_sensitive=False), help="Filter competitions by their format.")
@click.option("--mode", "mode_raw", required=False, type=click.Choice([x.name for x in CompetitionMode], case_sensitive=False), help="Filter competitions by their mode.")
@click.option("--status", "status_raw", required=False, type=click.Choice([x.name for x in CompetitionStatus], case_sensitive=False), help="Filter competitions by their status.")
@click.option("--continuous", required=False, type=bool, help="Filter continuous competitions.")
@click.option("--external", required=False, type=bool, help="Filter external competitions.")
@click.option("--featured", required=False, type=bool, help="Filter featured competitions.")
@click.option("--organizer-name", required=False, type=str, help="Filter competitions by their organizer's name.")
@click.option("--team-based", required=False, type=bool, help="Filter team based competitions.")
def competition_list(
    format_raw: str | None,
    status_raw: str | None,
    mode_raw: str | None,
    continuous: bool | None,
    external: bool | None,
    featured: bool | None,
    organizer_name: str | None,
    team_based: bool | None,
):
    from crunch.command.competition import competition_list

    competition_list(
        format=CompetitionFormat[format_raw.upper()] if format_raw else None,
        status=CompetitionStatus[status_raw.upper()] if status_raw else None,
        mode=CompetitionMode[mode_raw.upper()] if mode_raw else None,
        continuous=continuous,
        external=external,
        featured=featured,
        organizer_name=organizer_name,
        team_based=team_based,
    )


@competition_group.command(name="show", help="Show a competition.")
@click.argument("competition-name", required=True)
def competition_show(
    competition_name: str,
):
    from crunch.command.competition import competition_show

    competition_show(
        competition_identifier=competition_name,
    )


@cli.group(name="quickstarter", help="Manage quickstarters.")
def quickstarter_group():
    pass


@quickstarter_group.command(name="list", help="List available quickstarters.")
def quickstarter_list():
    from crunch.command.quickstarter import quickstarter_list

    repository = _open_repository()

    quickstarter_list(
        repository,
    )


@quickstarter_group.command(name="show", help="Show a quickstarter.")
@click.argument("quickstarter_name", required=True)
def quickstarter_show(
    quickstarter_name: str,
):
    from crunch.command.quickstarter import quickstarter_show

    repository = _open_repository()

    quickstarter_show(
        repository,
        quickstarter_name=quickstarter_name,
    )


@quickstarter_group.command(name="apply", help="Download a quickstarter locally.")
@click.argument("quickstarter_name", required=True)
@click.option("--overwrite", is_flag=True)
def quickstarter_apply(
    quickstarter_name: str,
    overwrite: bool,
):
    from crunch.command.quickstarter import quickstarter_apply

    repository = _open_repository()

    quickstarter_apply(
        repository,
        quickstarter_name=quickstarter_name,
        overwrite=overwrite,
    )


@cli.group(name="submission", help="Manage submissions.")
def submission_group():
    pass


@submission_group.command(name="list", help="List submissions.")
@click.option("--limit", type=int, default=10)
@click.option("--all", is_flag=True)
def submission_list(
    limit: int,
    all: bool,
):
    from crunch.command.submission import submission_list

    repository = _open_repository()

    submission_list(
        repository,
        limit=limit if not all else None,
    )


@submission_group.command(name="show", help="Show a submission.")
@click.argument("submission", type=SubmissionIdentifierClickType(), required=True)
def submission_show(
    submission: "SubmissionIdentifierType",
):
    from crunch.command.submission import submission_show

    repository = _open_repository()

    submission_show(
        repository,
        submission_identifier=submission,
    )


@submission_group.command(name="checkout", help="Check out files (or everything) from a submission.")
@click.argument("submission", type=SubmissionIdentifierClickType())
@click.argument("files", nargs=-1)
def submission_checkout(
    submission: "SubmissionIdentifierType",
    files: Sequence[str],
):
    # resolve before chdir
    from crunch.command.submission import submission_checkout

    absolute_files = [
        os.path.abspath(file)
        for file in files
    ]

    repository = _open_repository()

    submission_checkout(
        repository,
        submission_identifier=submission,
        files=absolute_files if len(absolute_files) else None,
    )


@cli.group(name="runtime", help="Manage runtimes.")
def runtime_group():
    pass


@runtime_group.command(name="list", help="List runtimes.")
@click.option("--submission", type=SubmissionIdentifierClickType(), default="@last")
def runtime_list(
    submission: "SubmissionIdentifierType",
):
    from crunch.command.runtime import runtime_list

    repository = _open_repository()

    runtime_list(
        repository,
        submission_identifier=submission,
    )


@runtime_group.command(name="request", help="Request a runtime option.")
@click.argument("runtime", type=str, required=True)
@click.option("--submission", type=SubmissionIdentifierClickType(), default="@last")
@click.option("--justification", type=str, required=True)
def runtime_request(
    runtime: str,
    submission: "SubmissionIdentifierType",
    justification: str,
):
    from crunch.command.runtime import runtime_request

    repository = _open_repository()

    runtime_request(
        repository,
        runtime_option_name=runtime,
        submission_identifier=submission,
        justification=justification,
    )


@cli.group(name="run", help="Manage runs.")
def run_group():
    pass


@run_group.command(name="create", help="Create a run.")
@click.option("--submission", type=SubmissionIdentifierClickType(), default="@last")
@click.option("--train-frequency", type=int, required=False)
@click.option("--force-first-train", type=bool, required=False)
@click.option("--runtime", type=str, required=True)
def run_create(
    submission: "SubmissionIdentifierType",
    train_frequency: int | None,
    force_first_train: bool | None,
    runtime: str,
):
    from crunch.command.run import run_create

    repository = _open_repository()

    run_create(
        repository,
        submission_identifier=submission,
        train_frequency=train_frequency,
        force_first_train=force_first_train,
        runtime_definition_name=runtime,
    )


@run_group.command(name="list", help="List runs.")
@click.option("--limit", type=int, default=10)
@click.option("--all", is_flag=True)
def run_list(
    limit: int,
    all: bool,
):
    from crunch.command.run import run_list

    repository = _open_repository()

    run_list(
        repository,
        limit=limit if not all else None,
    )


@run_group.command(name="show", help="Show a run.")
@click.argument("run_id", type=int, required=True)
def run_show(
    run_id: int,
):
    from crunch.command.run import run_show

    repository = _open_repository()

    run_show(
        repository,
        run_id=run_id,
    )


@run_group.command(name="logs", help="Show the logs of a run.")
@click.option("-t", "--tail", type=int, required=False)
@click.option("--follow", is_flag=True)
@click.option("--debug", is_flag=True)
@click.argument("run_id", type=int, required=True)
def run_logs(
    run_id: int,
    tail: int | None,
    follow: bool,
    debug: bool,
):
    from crunch.command.run import run_logs

    repository = _open_repository()

    run_logs(
        repository,
        run_id=run_id,
        tail=tail,
        follow=follow,
        debug=debug,
    )


@run_group.command(name="wait", help="Wait for a run to complete. Exit code: 0 = succeeded, 1 = failed, 2 = timed out (still running), 3 = terminated.")
@click.option("--timeout", type=int, required=False)
@click.option("--poll-interval", type=int, default=10)
@click.argument("run_id", type=int, required=True)
def run_wait(
    run_id: int,
    timeout: int | None,
    poll_interval: int,
):
    from crunch.command.run import run_wait

    repository = _open_repository()

    exit(run_wait(
        repository,
        run_id=run_id,
        timeout=timeout,
        poll_interval=poll_interval,
    ))


@run_group.command(name="select", help="Select a run for the Out-of-Sample.")
@click.argument("run_id", type=int, required=True)
def run_select(
    run_id: int,
):
    from crunch.command.run import run_select

    repository = _open_repository()

    run_select(
        repository,
        run_id=run_id,
    )


@run_group.command(name="terminate", help="Terminate a run.")
@click.argument("run_id", type=int, required=True)
def run_terminate(run_id: int):
    from crunch.command.run import run_terminate

    repository = _open_repository()

    run_terminate(
        repository,
        run_id=run_id,
    )


@cli.group(name="whitelist", help="Manage whitelisted libraries.")
def whitelist_group():
    pass


@whitelist_group.command(name="search", help="Search whitelisted libraries.")
@click.option("--language", type=ProgrammingLanguageClickType())
@click.argument("query", type=str, required=True)
@click.option("--limit", type=int, default=10)
@click.option("--all", is_flag=True)
def whitelist_search(
    query: str,
    language: "RequirementLanguage | None",
    limit: int,
    all: bool,
):
    from crunch.command.whitelist import whitelist_search

    exit(whitelist_search(
        query=query,
        language=language,
        limit=limit if not all else None,
    ))


@whitelist_group.command(name="validate", help="Validate a requirements.txt file.")
@click.option("--language", type=ProgrammingLanguageClickType())
@click.argument("requirements_txt_file_path", type=str, required=False)
def whitelist_validate(
    requirements_txt_file_path: str | None,
    language: "RequirementLanguage | None",
):
    from crunch.command.whitelist import whitelist_validate

    if language is None:
        from crunch_convert import RequirementLanguage
        language = RequirementLanguage.PYTHON

    if requirements_txt_file_path is None:
        requirements_txt_file_path = language.txt_file_name

    exit(whitelist_validate(
        requirements_txt_file_path=requirements_txt_file_path,
        language=language,
    ))


@whitelist_group.command(name="request", help="Request a library to be whitelisted.")
@click.option("--name", type=str)
@click.option("--alias", type=str)
@click.option("--language", type=ProgrammingLanguageClickType())
@click.option("--gpu", "gpu_requirement", type=GpuRequirementClickType())
@click.option("--additional-details", type=str)
def whitelist_request(
    name: str | None,
    alias: str | None,
    language: "RequirementLanguage | None",
    gpu_requirement: "GpuRequirement | None",
    additional_details: str | None,
):
    from crunch.command.whitelist import whitelist_request

    if not name and not alias:
        raise click.UsageError("at least one of `--name` or `--alias` must be provided")

    repository = _open_repository()

    whitelist_request(
        repository,
        name=name,
        alias=alias,
        language=language,
        gpu_requirement=gpu_requirement,
        additional_details=additional_details,
    )


@cli.group(name="runner", help="(internal use only)")
def runner_group():
    pass


@runner_group.command(help="Run your code locally.")
@click.option("--main-file", "main_file_path", default=constants.DEFAULT_MAIN_FILE_PATH, show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop.")
@click.option("--train-frequency", default=1, show_default=True, help="Train interval.")
@click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check.")
@click.option("--round-number", default="@current", help="Change round number to get the data from.")
@click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`.")
@click.option("--no-determinism-check", is_flag=True, help="Disable the determinism check.")
def local(
    main_file_path: str,
    model_directory_path: str,
    no_force_first_train: bool,
    train_frequency: int,
    skip_library_check: bool,
    round_number: RoundIdentifierType,
    has_gpu: bool,
    no_determinism_check: bool | None,
):
    from crunch.command.test import test

    from . import library, tester

    repository = _open_repository()
    tester.install_logger()

    if not skip_library_check and os.path.exists(constants.REQUIREMENTS_TXT):
        library.scan(logger=tester.logger)

        tester.logger.warning('')

    if no_determinism_check == False:
        no_determinism_check = None

    with convert_if_necessary(main_file_path):
        test(
            repository,
            main_file_path,
            model_directory_path,
            not no_force_first_train,
            train_frequency,
            round_number,
            has_gpu,
            no_determinism_check,
        )


@runner_group.command(help="Cloud runner, do not directly run!")
@click.option("--competition-name", envvar="COMPETITION_NAME", required=True)
# ---
@click.option("--context-directory", envvar="CONTEXT_DIRECTORY", default="/context")
@click.option("--scoring-directory", envvar="SCORING_DIRECTORY", default="{context}/scoring")
@click.option("--venv-directory", envvar="VENV_DIRECTORY", default="{context}/venv")
@click.option("--data-directory", envvar="DATA_DIRECTORY", default="{context}/data")
@click.option("--code-directory", envvar="CODE_DIRECTORY", default="{context}/code")
@click.option("--model-directory", envvar="MODEL_DIRECTORY", default="model")
@click.option("--prediction-directory", envvar="PREDICTION_DIRECTORY", default="{context}/prediction")
@click.option("--main-file", envvar=constants.MAIN_FILE_PATH_ENV_VAR, default=constants.DEFAULT_MAIN_FILE_PATH)
# ---
@click.option("--run-id", envvar="RUN_ID", required=True, type=int)
@click.option("--run-token", envvar="RUN_TOKEN", required=True)
@click.option("--log-secret", envvar="LOG_SECRET", default=None, type=str)
@click.option("--train-frequency", envvar="TRAIN_FREQUENCY", type=int, default=0)
@click.option("--force-first-train", envvar="FORCE_FIRST_TRAIN", type=bool, default=False)
@click.option("--determinism-check", "determinism_check_enabled", envvar="DETERMINISM_CHECK", type=bool, default=False)
@click.option("--gpu", envvar="GPU", type=bool, default=False)
@click.option("--phase-type", "phase_type_string", envvar="PHASE_TYPE", type=click.Choice([PhaseType.SUBMISSION.name, PhaseType.OUT_OF_SAMPLE.name]), default=PhaseType.SUBMISSION.name)
@click.option("--chain-height", envvar="CHAIN_HEIGHT", type=int, default=1)
@click.option("--crunch-cli-commit-hash", default="master", envvar="CRUNCH_CLI_COMMIT_HASH")
# ---
@click.option("--max-retry", envvar="MAX_RETRY", default=3, type=int)
@click.option("--retry-seconds", envvar="RETRY_WAIT", default=60, type=int)
def cloud(
    competition_name: str,
    # ---
    context_directory: str,
    scoring_directory: str,
    venv_directory: str,
    data_directory: str,
    code_directory: str,
    model_directory: str,
    prediction_directory: str,
    main_file: str,
    # ---
    run_id: int,
    run_token: str,
    log_secret: str,
    train_frequency: int,
    force_first_train: bool,
    determinism_check_enabled: bool,
    gpu: bool,
    phase_type_string: str,
    chain_height: int,
    crunch_cli_commit_hash: str,
    # ---
    max_retry: int,
    retry_seconds: int
):
    from .runner import is_inside
    if not is_inside:
        print("not in a runner")
        raise click.Abort()

    os.unsetenv("RUN_ID")
    os.unsetenv("RUN_TOKEN")
    os.unsetenv("LOG_SECRET")

    phase_type = PhaseType[phase_type_string]

    code_directory = code_directory.replace("{context}", context_directory)
    prediction_directory = prediction_directory.replace("{context}", context_directory)
    scoring_directory = scoring_directory.replace("{context}", context_directory)
    data_directory = data_directory.replace("{context}", context_directory)
    venv_directory = venv_directory.replace("{context}", context_directory)

    model_directory_path = os.path.join(code_directory, model_directory)

    auth = api.Auth.run_token(run_token)
    client = api.Client.from_env(auth)

    competition = client.competitions.get(competition_name)
    run = client.get_runner_run(run_id)

    from .runner.cloud import CloudRunner
    runner = CloudRunner(
        competition,
        run,
        client,
        # ---
        context_directory,
        scoring_directory,
        venv_directory,
        data_directory,
        code_directory,
        main_file,
        # ---
        model_directory_path,
        prediction_directory,
        # ---
        log_secret,
        train_frequency,
        force_first_train,
        determinism_check_enabled,
        gpu,
        phase_type,
        chain_height,
        crunch_cli_commit_hash,
        # ---
        max_retry,
        retry_seconds,
    )

    runner.start()


@runner_group.command(help="Cloud executor, do not directly run!")
@click.option("--competition", "competition_name", required=True)
# ---
@click.option("--data-directory", "data_directory_path", default=None)
@click.option("--main-file", required=True)
@click.option("--code-directory", required=True)
@click.option("--model-directory", "model_directory_path", required=True)
@click.option("--prediction-directory", "prediction_directory_path", required=True)
@click.option("--trace", "trace_path", required=True)
@click.option("--ping-url", "ping_urls", multiple=True)
# ---
@click.option("--train", required=True, type=bool)
@click.option("--gpu", required=True, type=bool)
# ---
@click.option("--fuse-pid", required=True, type=int)
@click.option("--fuse-signal-number", required=True, type=int)
@click.option("--exit-file", "exit_file_path", required=True, type=str)
@click.option("--exit-content", required=True, type=str)
# ---
@click.option("--runner-py-file", "runner_dot_py_file_path", type=str, required=True)
@click.option("--command", "command_name", type=str, required=True)
@click.option("--parameters", "parameters_json_string", type=str, required=True)
def cloud_executor(
    competition_name: str,
    # ---
    data_directory_path: str,
    main_file: str,
    code_directory: str,
    model_directory_path: str,
    prediction_directory_path: str,
    trace_path: str,
    ping_urls: list[str],
    # ---
    train: bool,
    gpu: bool,
    # ---
    fuse_pid: int,
    fuse_signal_number: int,
    exit_file_path: str,
    exit_content: str,
    # ---
    runner_dot_py_file_path: str,
    command_name: str,
    parameters_json_string: str,
):
    from .runner import is_inside
    if not is_inside:
        print("not in a runner")
        raise click.Abort()

    from . import monkey_patches
    monkey_patches.apply_all()

    from .runner.cloud_executor import SandboxExecutor
    executor = SandboxExecutor(
        competition_name,
        # ---
        data_directory_path,
        main_file,
        code_directory,
        model_directory_path,
        prediction_directory_path,
        trace_path,
        ping_urls,
        # ---
        train,
        gpu,
        # ---
        fuse_pid,
        fuse_signal_number,
        # ---
        runner_dot_py_file_path,
        command_name,
        json.loads(parameters_json_string) if parameters_json_string else {},
    )

    executor.start()

    with open(exit_file_path, "w") as fd:
        fd.write(exit_content)


@cli.group(name="organizer", help="(internal use only)")
@click.argument('competition-name')
@click.pass_context
def organize_group(
    context: click.Context,
    competition_name: str,
):
    _remember_competition_name(competition_name)

    client = api.Client.from_env()

    competition = client.competitions.get(competition_name)
    context.obj = competition


organize_group.add_command(organize_test_group)

cli.add_command(local, "test")
cli.add_command(competition_list, "list")
