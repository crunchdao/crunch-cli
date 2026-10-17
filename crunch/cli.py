import functools
import json
import os
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence

import click

from crunch.api import CompetitionFormat, CompetitionMode, CompetitionStatus, PhaseType, RoundIdentifierType
from crunch.command import SetupSubmissionNumberClickType, SetupSubmissionNumberType
from crunch.command.init import ApiKeySetupCredentials, CloneTokenSetupCredentials, SetupCredentials
from crunch.command.submission import SubmissionIdentifierClickType
from crunch.command.whitelist import GpuRequirementClickType, ProgrammingLanguageClickType
from crunch.notebook import NotebookSecretNotAccessibleError
from crunch.repository import Repository, RepositoryError
from crunch.runner.types import KwargsLike
from crunch.unstructured.cli import organize_test_group

from . import __version__, api, command, constants, store

store.load_from_env()

if TYPE_CHECKING:
    from crunch_convert import RequirementLanguage

    from crunch.api import GpuRequirement, SubmissionIdentifierType


DIRECTORY_DEFAULT_FORMAT = "{competitionName}-{projectName}"


ENVIRONMENT_PRODUCTION = "production"
ENVIRONMENT_STAGING = "staging"
ENVIRONMENT_DEVELOPMENT = "development"

ENVIRONMENT_ALIASES = {
    "prod": ENVIRONMENT_PRODUCTION,
    "test": ENVIRONMENT_STAGING,
    "dev": ENVIRONMENT_DEVELOPMENT,
    "local": ENVIRONMENT_DEVELOPMENT,
}

ENVIRONMENTS = {
    ENVIRONMENT_PRODUCTION: (constants.API_BASE_URL_PRODUCTION, constants.WEB_BASE_URL_PRODUCTION),
    ENVIRONMENT_STAGING: (constants.API_BASE_URL_STAGING, constants.WEB_BASE_URL_STAGING),
    ENVIRONMENT_DEVELOPMENT: (constants.API_BASE_URL_DEVELOPMENT, constants.WEB_BASE_URL_DEVELOPMENT),
}

DATA_SIZE_VARIANTS = [
    api.SizeVariant.DEFAULT.name.lower(),
    api.SizeVariant.SMALL.name.lower(),
    api.SizeVariant.LARGE.name.lower(),
]


def _format_directory(directory: str, competition_name: str, project_name: str):
    directory = directory \
        .replace("{competitionName}", competition_name) \
        .replace("{projectName}", project_name)

    return os.path.normpath(directory)


def _echo_version():
    click.echo(f"{__version__.__title__}, version {__version__.__version__}")


show_tips: bool = False


def _open_repository() -> Repository:
    repository = Repository.open()

    os.chdir(repository.root_directory_path)
    return repository


_COMPETITION_NAME_META_KEY = "crunch.competition_name"


def _remember_competition_name(competition_name: str) -> None:
    click.get_current_context().meta[_COMPETITION_NAME_META_KEY] = competition_name


API_KEY_CLONE_TOKEN = "@api-key"
_token_option = click.option("--token", "clone_token", required=True, help=f"Clone token to use, or `{API_KEY_CLONE_TOKEN}` to authenticate with the CRUNCH_API_KEY secret of the notebook platform (Kaggle, Colab, ...).")


def _create_setup_credentials(
    *,
    clone_token: str,
    competition_name: str,
    project_name: Optional[str],
) -> SetupCredentials:
    if clone_token == API_KEY_CLONE_TOKEN:
        if project_name is None:
            return ApiKeySetupCredentials(competition_name)

        return ApiKeySetupCredentials(competition_name, project_name)

    return CloneTokenSetupCredentials(clone_token, project_name)


class _CliGroup(click.Group):

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except RepositoryError as error:
            print(f"project: {error}", file=sys.stderr)
            raise click.Abort()
        except NotebookSecretNotAccessibleError as error:
            print(f"secret: {error}", file=sys.stderr)
            raise click.Abort()
        except api.ApiException as error:
            print("\n---")
            error.print_helper(
                competition_name=ctx.meta.get(_COMPETITION_NAME_META_KEY),
            )
            ctx.exit(1)


@click.group(cls=_CliGroup)
@click.version_option(__version__.__version__, package_name="__version__.__title__")
@click.option("--debug", envvar=constants.DEBUG_ENV_VAR, is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", envvar=constants.API_BASE_URL_ENV_VAR, default=constants.API_BASE_URL_PRODUCTION, help="Set the API base url.")
@click.option("--web-base-url", envvar=constants.WEB_BASE_URL_ENV_VAR, default=constants.WEB_BASE_URL_PRODUCTION, help="Set the Web base url.")
@click.option("--competitions", "competitions_source", envvar=constants.COMPETITIONS_SOURCE_ENV_VAR, default=constants.COMPETITIONS_SOURCE_DEFAULT, help="Set the Competitions repository location.")
@click.option("--environment", "--env", "environment_name", envvar=constants.ENVIRONMENT_ENV_VAR, help="Connect to another environment.")
@click.option("--no-tips", is_flag=True, help="Disable tips.")
def cli(
    debug: bool,
    api_base_url: str,
    web_base_url: str,
    competitions_source: str,
    environment_name: str,
    no_tips: bool,
):
    constants.RUN_VIA_CLI = True

    store.debug = debug
    store.api_base_url = api_base_url
    store.web_base_url = web_base_url
    store.competitions_source = competitions_source

    global show_tips
    show_tips = not no_tips

    environment_name = ENVIRONMENT_ALIASES.get(environment_name) or environment_name
    if environment_name in ENVIRONMENTS:
        print(f"environment: forcing {environment_name} urls, ignoring ${constants.API_BASE_URL_ENV_VAR} and ${constants.WEB_BASE_URL_ENV_VAR}")

        store.api_base_url, store.web_base_url = ENVIRONMENTS[environment_name]
    elif environment_name:
        print(f"environment: unknown environment `{environment_name}`, ignoring it")


@cli.command(help="Test if the server is online.")
def ping():
    client = api.Client.from_env()

    try:
        client.competitions.get(1)
        print("server is up!")
    except BaseException as exception:
        print(f"server is down? ({exception})")


@cli.command(name="list", help="List all available competitions.")
@click.option("--format", "format_raw", required=False, type=click.Choice([x.name for x in CompetitionFormat], case_sensitive=False), help="Filter competitions by their format.")
@click.option("--mode", "mode_raw", required=False, type=click.Choice([x.name for x in CompetitionMode], case_sensitive=False), help="Filter competitions by their mode.")
@click.option("--status", "status_raw", required=False, type=click.Choice([x.name for x in CompetitionStatus], case_sensitive=False), help="Filter competitions by their status.")
@click.option("--continuous", required=False, type=bool, help="Filter continuous competitions.")
@click.option("--external", required=False, type=bool, help="Filter external competitions.")
@click.option("--featured", required=False, type=bool, help="Filter featured competitions.")
@click.option("--organizer-name", required=False, type=str, help="Filter competitions by their organizer's name.")
@click.option("--team-based", required=False, type=bool, help="Filter team based competitions.")
def list_competitions(
    format_raw: Optional[str],
    status_raw: Optional[str],
    mode_raw: Optional[str],
    continuous: Optional[bool],
    external: Optional[bool],
    featured: Optional[bool],
    organizer_name: Optional[str],
    team_based: Optional[bool],
):
    format = CompetitionFormat[format_raw.upper()] if format_raw else None
    mode = CompetitionMode[mode_raw.upper()] if mode_raw else None
    status = CompetitionStatus[status_raw.upper()] if status_raw else None

    client = api.Client.from_env()

    competitions = client.competitions.list(
        format=format,
        status=status,
        mode=mode,
        continuous=continuous,
        external=external,
        featured=featured,
        organizer_name=organizer_name,
        team_based=team_based,
    )

    if not competitions:
        print("No competitions found.")
        return

    print("Available competitions:")
    for competition in competitions:
        category = "external" if competition.external else competition.format.name.lower()
        print(f"- {competition.name} ({category})")


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
    _remember_competition_name(competition_name)

    setup_credentials = _create_setup_credentials(
        clone_token=clone_token,
        competition_name=competition_name,
        project_name=project_name,
    )

    directory = _format_directory(directory, competition_name, setup_credentials.get_project_name())

    try:
        repository = command.init(
            setup_credentials=setup_credentials,
            directory=directory,
            model_directory=model_directory_path,
            force=force,
        )

        if not no_data:
            command.download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        command.download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly initialized.")


@cli.command(help="Setup a workspace directory.")
@_token_option
@click.option("--submission", "submission_number", required=False, type=SetupSubmissionNumberClickType(), default="latest", help='Submission number to clone. ("latest" if not specified, "scratch" to disable)')
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--no-model", is_flag=True, help="Do not download the model of the cloned submission.")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--notebook", is_flag=True, help="Setup everything for a notebook environment.")
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
    notebook: bool,
    data_size_variant_raw: str,
):
    _remember_competition_name(competition_name)

    setup_credentials = _create_setup_credentials(
        clone_token=clone_token,
        competition_name=competition_name,
        project_name=project_name,
    )

    _echo_version()

    if notebook:
        if force:
            print("notebook `--force` is implicit", file=sys.stderr)

        if directory != DIRECTORY_DEFAULT_FORMAT:
            print("notebook `[directory]` is forced to '.'", file=sys.stderr)

        force = True
        directory = "."
    else:
        directory = _format_directory(directory, competition_name, setup_credentials.get_project_name())

    data_size_variant = api.SizeVariant[data_size_variant_raw.upper()]

    try:
        repository = command.setup(
            setup_credentials,
            submission_number,
            directory,
            model_directory_path,
            force,
            no_model,
            data_size_variant,
        )

        if not no_data:
            command.download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        command.download_no_data_available()

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")

    if directory != '.':
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
    project_name: Optional[str],
):
    _remember_competition_name(competition_name)

    if clone_token != API_KEY_CLONE_TOKEN and project_name is not None:
        raise click.UsageError(f"a project name can only be given with `{API_KEY_CLONE_TOKEN}`, a clone token already identifies its project")

    setup_credentials = _create_setup_credentials(
        clone_token=clone_token,
        competition_name=competition_name,
        project_name=project_name,
    )

    _echo_version()

    directory = os.getcwd()

    data_size_variant = api.SizeVariant[data_size_variant_raw.upper()]

    try:
        repository = command.setup_notebook(
            setup_credentials,
            submission_number,
            directory,
            model_directory_path,
            no_model,
            data_size_variant,
        )

        if not no_data:
            command.download(repository, force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        command.download_no_data_available()

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
    converted = False
    if not os.path.exists(main_file_path):
        print(f"missing {main_file_path}")

        file_name, _ = os.path.splitext(os.path.basename(main_file_path))
        dirpath = os.path.dirname(main_file_path)
        path_without_extension = os.path.join(dirpath, file_name)

        notebook_file_path = f"{path_without_extension}.ipynb"
        if not os.path.exists(notebook_file_path):
            raise click.Abort()

        command.convert(notebook_file_path, main_file_path)
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
@click.option("--export", "export_path", show_default=True, type=str, help="Copy the `.tar` to the specified file.")
@click.option("--no-pip-freeze", is_flag=True, help="Do not do a `pip freeze` to know preferred packages version.")
@click.option("--dry", is_flag=True, help="Prepare file but do not really create the submission.")
def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
    export_path: Optional[str],
    no_pip_freeze: bool,
    dry: bool,
):
    repository = _open_repository()

    if export_path is not None:
        print("--export is not supported anymore", file=sys.stderr)
        raise click.Abort()

    with convert_if_necessary(main_file_path):
        command.push(
            repository=repository,
            message=message,
            main_file_path=main_file_path,
            model_directory_relative_path=model_directory_path,
            include_installed_packages_version=not no_pip_freeze,
            no_afterword=False,
            dry=dry,
        )


def local_options(f: Callable[..., Any]) -> Callable[..., Any]:
    options: List[Callable[..., Callable[..., Any]]] = [
        click.option("--main-file", "main_file_path", default=constants.DEFAULT_MAIN_FILE_PATH, show_default=True, help="Entrypoint of your code."),
        click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored."),
        click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop."),
        click.option("--train-frequency", default=1, show_default=True, help="Train interval."),
        click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check."),
        click.option("--round-number", default="@current", help="Change round number to get the data from."),
        click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`."),
        click.option("--no-determinism-check", is_flag=True, help="Disable the determinism check."),
    ]

    return functools.reduce(lambda f, option: option(f), options, f)


@cli.command(help="Test your code locally.")
@local_options
@click.pass_context
def test(
    context: click.Context,
    **kwargs: KwargsLike,
):
    context.forward(local, **kwargs)


@cli.command(help="Download the data locally.")
@click.option("--round-number", default="@current")
@click.option("--force", is_flag=True, help="Force the download of the data.")
@click.option("--size-variant", "size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), required=False, help="Use alternative version of the data.")
def download(
    round_number: "RoundIdentifierType",
    force: bool,
    size_variant_raw: Optional[str],
):
    repository = _open_repository()

    size_variant = (
        api.SizeVariant[size_variant_raw.upper()]
        if size_variant_raw is not None
        else None
    )

    try:
        command.download(
            repository,
            round_number,
            force,
            size_variant,
        )
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
        command.download_no_data_available()


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
    command.convert(
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
    if not clone_token:
        clone_token = click.prompt("Clone Token", hide_input=True)

    repository = _open_repository()

    command.update_token(
        repository,
        clone_token=clone_token
    )


@cli.command(help="Show information on the competition.")
def status():
    repository = _open_repository()

    command.status(
        repository,
        show_tips=show_tips,
    )


@cli.command(help="Show the project's rank on the leaderboard.")
def leaderboard():
    repository = _open_repository()

    command.leaderboard(
        repository,
        show_tips=show_tips,
    )


@cli.command(help="Show the project's quota.")
def quota():
    repository = _open_repository()

    command.quota(
        repository,
        show_tips=show_tips,
    )


@cli.group(name="quickstarter", help="Manage quickstarters.")
def quickstarter_group():
    pass


@quickstarter_group.command(name="list", help="List available quickstarters.")
def quickstarter_list():
    repository = _open_repository()

    command.quickstarter_list(
        repository,
        show_tips=show_tips,
    )


@quickstarter_group.command(name="show", help="Show a quickstarter.")
@click.argument("quickstarter_name", required=True)
def quickstarter_show(
    quickstarter_name: str,
):
    repository = _open_repository()

    command.quickstarter_show(
        repository,
        quickstarter_name=quickstarter_name,
        show_tips=show_tips,
    )


@quickstarter_group.command(name="apply", help="Download a quickstarter locally.")
@click.argument("quickstarter_name", required=True)
@click.option("--overwrite", is_flag=True)
def quickstarter_apply(
    quickstarter_name: str,
    overwrite: bool,
):
    repository = _open_repository()

    command.quickstarter_apply(
        repository,
        quickstarter_name=quickstarter_name,
        overwrite=overwrite,
        show_tips=show_tips,
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
    repository = _open_repository()

    command.submission_list(
        repository,
        limit=limit if not all else None,
        show_tips=show_tips,
    )


@submission_group.command(name="show", help="Show a submission.")
@click.argument("submission", type=SubmissionIdentifierClickType(), required=True)
def submission_show(
    submission: "SubmissionIdentifierType",
):
    repository = _open_repository()

    command.submission_show(
        repository,
        submission_identifier=submission,
        show_tips=show_tips,
    )


@submission_group.command(name="checkout", help="Check out files (or everything) from a submission.")
@click.argument("submission", type=SubmissionIdentifierClickType())
@click.argument("files", nargs=-1)
def submission_checkout(
    submission: "SubmissionIdentifierType",
    files: Sequence[str],
):
    # resolve before chdir
    absolute_files = [
        os.path.abspath(file)
        for file in files
    ]

    repository = _open_repository()

    command.submission_checkout(
        repository,
        submission_identifier=submission,
        files=absolute_files if len(absolute_files) else None,
        show_tips=show_tips,
    )


@cli.group(name="runtime", help="Manage runtimes.")
def runtime_group():
    pass


@runtime_group.command(name="list", help="List runtimes.")
@click.option("--submission", type=SubmissionIdentifierClickType(), default="@last")
def runtime_list(
    submission: "SubmissionIdentifierType",
):
    repository = _open_repository()

    command.runtime_list(
        repository,
        submission_identifier=submission,
        show_tips=show_tips,
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
    repository = _open_repository()

    command.runtime_request(
        repository,
        runtime_option_name=runtime,
        submission_identifier=submission,
        justification=justification,
        show_tips=show_tips,
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
    train_frequency: Optional[int],
    force_first_train: Optional[bool],
    runtime: str,
):
    repository = _open_repository()

    command.run_create(
        repository,
        submission_identifier=submission,
        train_frequency=train_frequency,
        force_first_train=force_first_train,
        runtime_definition_name=runtime,
        show_tips=show_tips,
    )


@run_group.command(name="list", help="List runs.")
@click.option("--limit", type=int, default=10)
@click.option("--all", is_flag=True)
def run_list(limit: int, all: bool):
    repository = _open_repository()

    command.run_list(
        repository,
        limit=limit if not all else None,
        show_tips=show_tips,
    )


@run_group.command(name="show", help="Show a run.")
@click.argument("run_id", type=int, required=True)
def run_show(run_id: int):
    repository = _open_repository()

    command.run_show(
        repository,
        run_id=run_id,
        show_tips=show_tips,
    )


@run_group.command(name="logs", help="Show the logs of a run.")
@click.option("-t", "--tail", type=int, required=False)
@click.option("--follow", is_flag=True)
@click.option("--debug", is_flag=True)
@click.argument("run_id", type=int, required=True)
def run_logs(run_id: int, tail: Optional[int], follow: bool, debug: bool):
    repository = _open_repository()

    command.run_logs(
        repository,
        run_id=run_id,
        tail=tail,
        follow=follow,
        debug=debug,
        show_tips=show_tips,
    )


@run_group.command(name="wait", help="Wait for a run to complete. Exit code: 0 = succeeded, 1 = failed, 2 = timed out (still running), 3 = terminated.")
@click.option("--timeout", type=int, required=False)
@click.option("--poll-interval", type=int, default=10)
@click.argument("run_id", type=int, required=True)
def run_wait(run_id: int, timeout: Optional[int], poll_interval: int):
    repository = _open_repository()

    exit(command.run_wait(
        repository,
        run_id=run_id,
        timeout=timeout,
        poll_interval=poll_interval,
        show_tips=show_tips,
    ))


@run_group.command(name="select", help="Select a run for the Out-of-Sample.")
@click.argument("run_id", type=int, required=True)
def run_select(run_id: int):
    repository = _open_repository()

    command.run_select(
        repository,
        run_id=run_id,
        show_tips=show_tips,
    )


@run_group.command(name="terminate", help="Terminate a run.")
@click.argument("run_id", type=int, required=True)
def run_terminate(run_id: int):
    repository = _open_repository()

    command.run_terminate(
        repository,
        run_id=run_id,
        show_tips=show_tips,
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
    language: Optional["RequirementLanguage"],
    limit: int,
    all: bool,
):
    exit(command.whitelist_search(
        query=query,
        language=language,
        limit=limit if not all else None,
        show_tips=show_tips,
    ))


@whitelist_group.command(name="validate", help="Validate a requirements.txt file.")
@click.option("--language", type=ProgrammingLanguageClickType())
@click.argument("requirements_txt_file_path", type=str, required=False)
def whitelist_validate(
    requirements_txt_file_path: Optional[str],
    language: Optional["RequirementLanguage"],
):
    if language is None:
        from crunch_convert import RequirementLanguage
        language = RequirementLanguage.PYTHON

    if requirements_txt_file_path is None:
        requirements_txt_file_path = language.txt_file_name

    exit(command.whitelist_validate(
        requirements_txt_file_path=requirements_txt_file_path,
        language=language,
        show_tips=show_tips,
    ))


@whitelist_group.command(name="request", help="Request a library to be whitelisted.")
@click.option("--name", type=str)
@click.option("--alias", type=str)
@click.option("--language", type=ProgrammingLanguageClickType())
@click.option("--gpu", "gpu_requirement", type=GpuRequirementClickType())
@click.option("--additional-details", type=str)
def whitelist_request(
    name: Optional[str],
    alias: Optional[str],
    language: Optional["RequirementLanguage"],
    gpu_requirement: Optional["GpuRequirement"],
    additional_details: Optional[str],
):
    if not name and not alias:
        raise click.UsageError("at least one of `--name` or `--alias` must be provided")

    repository = _open_repository()

    command.whitelist_request(
        repository,
        name=name,
        alias=alias,
        language=language,
        gpu_requirement=gpu_requirement,
        additional_details=additional_details,
        show_tips=show_tips,
    )


@cli.group(name="runner")
def runner_group():
    pass


@runner_group.command(help="Run your code locally.")
@local_options
def local(
    main_file_path: str,
    model_directory_path: str,
    no_force_first_train: bool,
    train_frequency: int,
    skip_library_check: bool,
    round_number: RoundIdentifierType,
    has_gpu: bool,
    no_determinism_check: Optional[bool],
):
    from . import library, tester

    repository = _open_repository()
    tester.install_logger()

    if not skip_library_check and os.path.exists(constants.REQUIREMENTS_TXT):
        library.scan(logger=tester.logger)

        tester.logger.warning('')

    if no_determinism_check == False:
        no_determinism_check = None

    with convert_if_necessary(main_file_path):
        command.test(
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
    client = api.Client.from_env(auth, show_progress=False)

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
    ping_urls: List[str],
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


@cli.group(name="organizer")
@click.argument('competition_name')
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
