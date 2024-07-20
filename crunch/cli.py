import functools
import logging
import os
import sys
import typing

import click

from . import __version__, api, benchmark, command, constants, store, utils

store.load_from_env()


DIRECTORY_DEFAULT_FORMAT = "{competitionName}-{projectName}"


def _format_directory(directory: str, competition_name: str, project_name: str):
    directory = directory \
        .replace("{competitionName}", competition_name) \
        .replace("{projectName}", project_name)

    return os.path.normpath(directory)


@click.group()
@click.version_option(
    __version__.__version__,
    package_name="__version__.__title__"
)
@click.option("--debug", envvar=constants.DEBUG_ENV_VAR, is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", envvar=constants.API_BASE_URL_ENV_VAR, default=constants.API_BASE_URL_DEFAULT, help="Set the API base url.")
@click.option("--web-base-url", envvar=constants.WEB_BASE_URL_ENV_VAR, default=constants.WEB_BASE_URL_DEFAULT, help="Set the Web base url.")
@click.option("--staging", is_flag=True, help="Connect to the staging environment.")
def cli(
    debug: bool,
    api_base_url: str,
    web_base_url: str,
    staging: bool,
):
    store.debug = debug
    store.api_base_url = api_base_url
    store.web_base_url = web_base_url

    if staging:
        print("environment: forcing staging urls")
        print(f"environment: ignoring ${constants.API_BASE_URL_ENV_VAR} and ${constants.WEB_BASE_URL_ENV_VAR}")

        store.api_base_url = constants.API_BASE_URL_STAGING
        store.web_base_url = constants.WEB_BASE_URL_STAGING


@cli.command(help="Initialize an empty workspace directory.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
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
    directory = _format_directory(directory, competition_name, project_name)

    try:
        command.init(
            clone_token=clone_token,
            competition_name=competition_name,
            project_name=project_name,
            directory=directory,
            model_directory=model_directory_path,
            force=force,
        )

        if not no_data:
            command.download(force=True)
    except api.CrunchNotFoundException:
        command.download_no_data_available()
    except api.ApiException as error:
        utils.exit_via(
            error,
            competition_name=competition_name
        )

    print("\n---")
    print(f"Success! Your environment has been correctly initialized.")


@cli.command(help="Setup a workspace directory with the latest submission of you code.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
@click.option("--submission", "submission_number", required=False, type=int, help="Submission number to clone. (latest if not specified)")
@click.option("--no-data", is_flag=True, help="Do not download the data. (faster)")
@click.option("--no-model", is_flag=True, help="Do not download the model of the cloned submission.")
@click.option("--force", "-f", is_flag=True, help="Deleting the old directory (if any).")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--no-quickstarter", is_flag=True, help="Disable quickstarter selection.")
@click.option("--quickstarter-name", type=str, help="Pre-select a quickstarter.")
@click.option("--show-notebook-quickstarters", is_flag=True, help="Show quickstarters notebook in selection.")
@click.option("--notebook", is_flag=True, help="Setup everything for a notebook environment.")
@click.argument("competition-name", required=True)
@click.argument("project-name", required=True)
@click.argument("directory", default=DIRECTORY_DEFAULT_FORMAT)
def setup(
    clone_token: str,
    submission_number: str,
    no_data: bool,
    no_model: bool,
    force: bool,
    competition_name: str,
    project_name: str,
    directory: str,
    model_directory_path: str,
    no_quickstarter: bool,
    quickstarter_name: str,
    show_notebook_quickstarters: bool,
    notebook: bool,
):
    if notebook:
        if force:
            print("notebook `--force` is implicit", file=sys.stderr)

        if no_quickstarter:
            print("notebook `--no-quickstarter` is implicit", file=sys.stderr)

        if quickstarter_name:
            print("notebook `--quickstarter-name` is incompatible, ignoring it", file=sys.stderr)
            quickstarter_name = None

        if show_notebook_quickstarters:
            print("notebook `--show-notebook-quickstarters` is incompatible, ignoring it", file=sys.stderr)
            show_notebook_quickstarters = False

        if directory != DIRECTORY_DEFAULT_FORMAT:
            print("notebook `[directory]` is forced to '.'", file=sys.stderr)

        force = True
        no_quickstarter = True
        directory = "."
    else:
        directory = _format_directory(directory, competition_name, project_name)

    try:
        command.setup(
            clone_token,
            submission_number,
            competition_name,
            project_name,
            directory,
            model_directory_path,
            force,
            no_model,
            not no_quickstarter,
            quickstarter_name,
            show_notebook_quickstarters,
        )

        if not no_data:
            command.download(force=True)
    except api.CrunchNotFoundException:
        command.download_no_data_available()
    except api.ApiException as error:
        utils.exit_via(
            error,
            competition_name=competition_name
        )

    print("\n---")
    print(f"Success! Your environment has been correctly setup.")
    print(f"Next recommended actions:")

    if directory != '.':
        print(f" - To get inside your workspace directory, run: cd {directory}")

    print(f" - To see all of the available commands of the CrunchDAO CLI, run: crunch --help")


@cli.command(help="Setup a workspace directory with the latest submission of you code.")
@click.option("--name", type=str, help="Pre-select a quickstarter.")
@click.option("--show-notebook", is_flag=True, help="Show quickstarters notebook in selection.")
@click.option("--overwrite", is_flag=True, help="Overwrite any files that are conflicting.")
def quickstarter(
    name: str,
    show_notebook: bool,
    overwrite: bool,
):
    utils.change_root()

    try:
        command.quickstarter(
            name=name,
            show_notebook=show_notebook,
            overwrite=overwrite,
        )
    except api.ApiException as error:
        utils.exit_via(error)

    print(f"quickstarter deployed")


@cli.command(help="Send the new submission of your code.")
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
@click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--export", "export_path", show_default=True, type=str, help="Copy the `.tar` to the specified file.")
@click.option("--no-pip-freeze", is_flag=True, help="Do not do a `pip freeze` to know preferred packages version.")
@click.option("--dry", is_flag=True, help="Prepare file but do not really create the submission.")
def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
    export_path: str,
    no_pip_freeze: bool,
    dry: bool,
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
            message,
            main_file_path,
            model_directory_path,
            not no_pip_freeze,
            dry,
            export_path,
        )
    except api.ApiException as error:
        utils.exit_via(error)
    finally:
        if converted:
            os.unlink(main_file_path)


@cli.command(help="[DEPRECATED] Send a prediction as your submission.")
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
@click.argument("file-path")
def push_prediction(
    message: str,
    file_path: str,
):
    utils.change_root()

    try:
        command.push_prediction(
            message,
            file_path,
        )
    except api.ApiException as error:
        utils.exit_via(error)


def local_options(f):
    options = [
        click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code."),
        click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored."),
        click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop."),
        click.option("--train-frequency", default=1, show_default=True, help="Train interval."),
        click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check."),
        click.option("--round-number", default="@current", help="Change round number to get the data from."),
        click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`."),
        click.option("--no-checks", is_flag=True, help="Disable final predictions checks.")
    ]

    return functools.reduce(lambda f, option: option(f), options, f)


@cli.command(help="Test your code locally.")
@local_options
@click.pass_context
def test(
    context: click.Context,
    **kwargs
):
    context.forward(local, **kwargs)


@cli.command(help="Download the data locally.")
@click.option("--round-number", default="@current")
def download(
    round_number: str
):
    utils.change_root()

    try:
        command.download(
            round_number=round_number
        )
    except api.CrunchNotFoundException:
        command.download_no_data_available()
    except api.ApiException as error:
        utils.exit_via(error)


@cli.command(help="Convert a notebook to a python script.")
@click.option("--override", is_flag=True, help="Force overwrite of the python file.")
@click.argument("notebook-file-path", required=True)
@click.argument("python-file-path", default="main.py")
def convert(
    override: bool,
    notebook_file_path: str,
    python_file_path: str,
):
    try:
        command.convert(
            notebook_file_path=notebook_file_path,
            python_file_path=python_file_path,
            override=override,
        )
    except api.ApiException as error:
        utils.exit_via(error)


@cli.command(help="Update a project token.")
@click.argument("clone-token", required=False)
def update_token(
    clone_token: str,
):
    if not clone_token:
        clone_token = click.prompt("Clone Token", hide_input=True)

    utils.change_root()

    try:
        command.update_token(
            clone_token=clone_token
        )
    except api.ApiException as error:
        utils.exit_via(error)


@cli.group(name="benchmark")
def benchmark_group():
    pass


@benchmark_group.command(help="Benchmark the orthogonalization feature.")
@click.argument("prediction-path", type=click.Path(exists=True, dir_okay=False), required=True)
def orthogonalization(
    prediction_path: str,
):
    utils.change_root()

    try:
        prediction = utils.read(prediction_path)

        benchmark.orthogonalization(
            prediction=prediction
        )
    except api.ApiException as error:
        utils.exit_via(error)


@cli.group(name="runner")
def runner_group():
    pass


@runner_group.command(help="Run your code locally.")
@click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default="resources", show_default=True, help="Directory where your model is stored.")
@click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop.")
@click.option("--train-frequency", default=1, show_default=True, help="Train interval.")
@click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check.")
@click.option("--round-number", default="@current", help="Change round number to get the data from.")
@click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`.")
@click.option("--no-checks", is_flag=True, help="Disable final predictions checks.")
@click.option("--no-determinism-check", is_flag=True, help="Disable the determinism check.")
def local(
    main_file_path: str,
    model_directory_path: str,
    no_force_first_train: bool,
    train_frequency: int,
    skip_library_check: bool,
    round_number: str,
    has_gpu: bool,
    no_checks: bool,
    no_determinism_check: bool,
):
    from . import library, tester

    utils.change_root()
    tester.install_logger()

    if not skip_library_check and os.path.exists(constants.REQUIREMENTS_TXT):
        library.scan(requirements_file=constants.REQUIREMENTS_TXT)
        logging.warn('')

    try:
        command.test(
            main_file_path,
            model_directory_path,
            not no_force_first_train,
            train_frequency,
            round_number,
            has_gpu,
            not no_checks,
            not no_determinism_check,
        )
    except api.ApiException as error:
        utils.exit_via(error)


@runner_group.command(help="Cloud runner, do not directly run!")
@click.option("--competition-name", envvar="COMPETITION_NAME", required=True)
# ---
@click.option("--context-directory", envvar="CONTEXT_DIRECTORY", default="/context")
@click.option("--state-file", envvar="STATE_FILE", default="{context}/state.json")
@click.option("--venv-directory", envvar="VENV_DIRECTORY", default="{context}/venv")
@click.option("--data-directory", envvar="DATA_DIRECTORY", default="{context}/data")
@click.option("--code-directory", envvar="CODE_DIRECTORY", default="{context}/code")
@click.option("--model-directory", envvar="MODEL_DIRECTORY", default="model")
@click.option("--main-file", envvar="MAIN_FILE", default="main.py")
# ---
@click.option("--run-id", envvar="RUN_ID", required=True)
@click.option("--run-token", envvar="RUN_TOKEN", required=True)
@click.option("--log-secret", envvar="LOG_SECRET", default=None, type=str)
@click.option("--train-frequency", envvar="TRAIN_FREQUENCY", type=int, default=0)
@click.option("--force-first-train", envvar="FORCE_FIRST_TRAIN", is_flag=True)
@click.option("--determinism-check", "determinism_check_enabled", envvar="DETERMINISM_CHECK", is_flag=True)
@click.option("--gpu", envvar="GPU", is_flag=True)
@click.option("--crunch-cli-commit-hash", default="main", envvar="CRUNCH_CLI_COMMIT_HASH")
# ---
@click.option("--max-retry", envvar="MAX_RETRY", default=3, type=int)
@click.option("--retry-seconds", envvar="RETRY_WAIT", default=60, type=int)
def cloud(
    competition_name: str,
    # ---
    context_directory: str,
    state_file: str,
    venv_directory: str,
    data_directory: str,
    code_directory: str,
    model_directory: str,
    main_file: str,
    # ---
    run_id: str,
    run_token: str,
    log_secret: str,
    train_frequency: int,
    force_first_train: bool,
    determinism_check_enabled: bool,
    gpu: bool,
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

    code_directory = code_directory.replace("{context}", context_directory)
    data_directory = data_directory.replace("{context}", context_directory)
    venv_directory = venv_directory.replace("{context}", context_directory)
    state_file = state_file.replace("{context}", context_directory)

    requirements_txt_path = os.path.join(code_directory, "requirements.txt")
    model_directory_path = os.path.join(code_directory, model_directory)

    prediction_file_name = "prediction.csv"
    prediction_path = os.path.join(context_directory, prediction_file_name)

    trace_file_name = "trace.txt"
    trace_path = os.path.join(context_directory, trace_file_name)

    auth = api.auth.RunTokenAuth(run_token)
    client = api.Client.from_env(auth)

    competition = client.competitions.get(competition_name)
    run = client.get_runner_run(run_id)

    from .runner.cloud import CloudRunner
    runner = CloudRunner(
        competition,
        run,
        # ---
        context_directory,
        state_file,
        venv_directory,
        data_directory,
        code_directory,
        main_file,
        # ---
        requirements_txt_path,
        model_directory_path,
        prediction_path,
        trace_path,
        # ---
        log_secret,
        train_frequency,
        force_first_train,
        determinism_check_enabled,
        gpu,
        crunch_cli_commit_hash,
        # ---
        max_retry,
        retry_seconds
    )

    runner.start()


@runner_group.command(help="Cloud executor, do not directly run!")
@click.option("--competition-name", required=True)
@click.option("--competition-format", required=True)
# ---
@click.option("--x", "x_path", required=True)
@click.option("--y", "y_path", required=True)
@click.option("--y-raw", "y_raw_path", default=None)
@click.option("--orthogonalization-data", "orthogonalization_data_path", default=None)
# ---
@click.option("--main-file", required=True)
@click.option("--code-directory", required=True)
@click.option("--model-directory", "model_directory_path", required=True)
@click.option("--prediction", "prediction_path", required=True)
@click.option("--trace", "trace_path", required=True)
@click.option("--state-file", "state_file", required=True)
@click.option("--ping-url", "ping_urls", multiple=True)
# ---
@click.option("--train", required=True, type=bool)
@click.option("--moon", required=True, type=int)
@click.option("--embargo", required=True, type=int)
@click.option("--number-of-features", required=True, type=int)
@click.option("--gpu", required=True, type=bool)
# ---
@click.option("--id-column-name", required=True)
@click.option("--moon-column-name", required=True)
@click.option("--target", "targets", required=True, multiple=True, nargs=3)
def cloud_executor(
    competition_name: str,
    competition_format: str,
    # ---
    x_path: str,
    y_path: str,
    y_raw_path: str,
    orthogonalization_data_path: str,
    # ---
    main_file: str,
    code_directory: str,
    model_directory_path: str,
    prediction_path: str,
    trace_path: str,
    state_file: str,
    ping_urls: typing.List[str],
    # ---
    train: bool,
    moon: int,
    embargo: int,
    number_of_features: int,
    gpu: bool,
    # ---
    id_column_name: str,
    moon_column_name: str,
    targets: typing.List[typing.Tuple[str, str, str]],
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
        api.CompetitionFormat[competition_format],
        # ---
        x_path,
        y_path,
        y_raw_path,
        orthogonalization_data_path,
        # ---
        main_file,
        code_directory,
        model_directory_path,
        prediction_path,
        trace_path,
        state_file,
        ping_urls,
        # ---
        train,
        moon,
        embargo,
        number_of_features,
        gpu,
        # ---
        api.ColumnNames(
            id_column_name,
            moon_column_name,
            {
                api.TargetColumnNames(None, target_name, input, output)
                for target_name, input, output in targets
            }
        )
    )

    executor.start()


if __name__ == '__main__':
    cli()
