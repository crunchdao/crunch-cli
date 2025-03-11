import contextlib
import functools
import json
import os
import random
import sys
import traceback
import typing

import click
import pandas

from . import __version__, api, benchmark, command, constants, store, utils

store.load_from_env()


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
    api.SizeVariant.LARGE.name.lower(),
]


def _format_directory(directory: str, competition_name: str, project_name: str):
    directory = directory \
        .replace("{competitionName}", competition_name) \
        .replace("{projectName}", project_name)

    return os.path.normpath(directory)


@click.group()
@click.version_option(__version__.__version__, package_name="__version__.__title__")
@click.option("--debug", envvar=constants.DEBUG_ENV_VAR, is_flag=True, help="Enable debug output.")
@click.option("--api-base-url", envvar=constants.API_BASE_URL_ENV_VAR, default=constants.API_BASE_URL_PRODUCTION, help="Set the API base url.")
@click.option("--web-base-url", envvar=constants.WEB_BASE_URL_ENV_VAR, default=constants.WEB_BASE_URL_PRODUCTION, help="Set the Web base url.")
@click.option("--staging", is_flag=True, help="[DEPRECATED] Connect to the staging environment.")
@click.option("--environment", "--env", "environment_name", help="Connect to another environment.")
def cli(
    debug: bool,
    api_base_url: str,
    web_base_url: str,
    staging: bool,
    environment_name: str,
):
    store.debug = debug
    store.api_base_url = api_base_url
    store.web_base_url = web_base_url

    if staging:
        if environment_name:
            print("option `--staging` is deprecated, ignoring it", file=sys.stderr)
        else:
            print("option `--staging` is deprecated, prefer `--environment staging`", file=sys.stderr)
            environment_name = ENVIRONMENT_STAGING

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


@cli.command(help="Initialize an empty workspace directory.")
@click.option("--token", "clone_token", required=True, help="Clone token to use.")
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
    directory = _format_directory(directory, competition_name, project_name)

    try:
        command.init(
            clone_token,
            competition_name,
            project_name,
            directory,
            model_directory_path,
            force,
        )

        if not no_data:
            command.download(force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
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
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
@click.option("--no-quickstarter", is_flag=True, help="Disable quickstarter selection.")
@click.option("--quickstarter-name", type=str, help="Pre-select a quickstarter.")
@click.option("--show-notebook-quickstarters", is_flag=True, help="Show quickstarters notebook in selection.")
@click.option("--notebook", is_flag=True, help="Setup everything for a notebook environment.")
@click.option("--size", "data_size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), default=DATA_SIZE_VARIANTS[0], help="Use another data variant.")
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
    data_size_variant_raw: str,
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

    data_size_variant = api.SizeVariant[data_size_variant_raw.upper()]

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
            data_size_variant,
        )

        if not no_data:
            command.download(force=True)
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
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


@contextlib.contextmanager
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
@click.option("-m", "--message", prompt=True, default="", help="Specify the change of your code. (like a commit message)")
@click.option("--main-file", "main_file_path", default="main.py", show_default=True, help="Entrypoint of your code.")
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored.")
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

    with convert_if_necessary(main_file_path):
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
        click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY, show_default=True, help="Directory where your model is stored."),
        click.option("--no-force-first-train", is_flag=True, help="Do not force the train at the first loop."),
        click.option("--train-frequency", default=1, show_default=True, help="Train interval."),
        click.option("--skip-library-check", is_flag=True, help="Skip forbidden library check."),
        click.option("--round-number", default="@current", help="Change round number to get the data from."),
        click.option("--gpu", "has_gpu", is_flag=True, help="Set `has_gpu` parameter to `True`."),
        click.option("--no-checks", is_flag=True, help="Disable final predictions checks."),
        click.option("--no-determinism-check", is_flag=True, help="Disable the determinism check."),
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
@click.option("--force", is_flag=True, help="Force the download of the data.")
@click.option("--size-variant", "size_variant_raw", type=click.Choice(DATA_SIZE_VARIANTS), required=False, help="Use alternative version of the data.")
def download(
    round_number: str,
    force: bool,
    size_variant_raw: typing.Optional[str],
):
    utils.change_root()

    size_variant = (
        api.SizeVariant[size_variant_raw.upper()]
        if size_variant_raw is not None
        else None
    )

    try:
        command.download(
            round_number,
            force,
            size_variant,
        )
    except (api.CrunchNotFoundException, api.MissingPhaseDataException):
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
@local_options
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
        library.scan(
            requirements_file=constants.REQUIREMENTS_TXT,
            logger=tester.logger
        )

        tester.logger.warning('')

    if no_determinism_check == False:
        no_determinism_check = None

    with convert_if_necessary(main_file_path):
        try:
            command.test(
                main_file_path,
                model_directory_path,
                not no_force_first_train,
                train_frequency,
                round_number,
                has_gpu,
                not no_checks,
                no_determinism_check,
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

    prediction_file_name = "prediction.parquet"
    prediction_path = os.path.join(context_directory, prediction_file_name)

    trace_file_name = "trace.txt"
    trace_path = os.path.join(context_directory, trace_file_name)

    auth = api.auth.RunTokenAuth(run_token)
    client = api.Client.from_env(auth, show_progress=False)

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
@click.option("--split-key-type", required=True)
# ---
@click.option("--x", "x_path", default=None)
@click.option("--y", "y_path", default=None)
@click.option("--y-raw", "y_raw_path", default=None)
@click.option("--orthogonalization-data", "orthogonalization_data_path", default=None)
@click.option("--data-directory", "data_directory_path", default=None)
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
@click.option("--loop-key", required=True, type=str)
@click.option("--embargo", required=True, type=int)
@click.option("--number-of-features", required=True, type=int)
@click.option("--gpu", required=True, type=bool)
# ---
@click.option("--id-column-name", required=True)
@click.option("--moon-column-name", required=True)
@click.option("--side-column-name", required=True)
@click.option("--input-column-name", required=True)
@click.option("--output-column-name", required=True)
@click.option("--target", "targets", required=True, multiple=True, nargs=5)
# ---
@click.option("--write-index", required=True, type=bool)
# ---
@click.option("--fuse-pid", required=True, type=int)
@click.option("--fuse-signal-number", required=True, type=int)
def cloud_executor(
    competition_name: str,
    competition_format: str,
    split_key_type: str,
    # ---
    x_path: str,
    y_path: str,
    y_raw_path: str,
    orthogonalization_data_path: str,
    data_directory_path: str,
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
    loop_key: str,
    embargo: int,
    number_of_features: int,
    gpu: bool,
    # ---
    id_column_name: str,
    moon_column_name: str,
    side_column_name: str,
    input_column_name: str,
    output_column_name: str,
    targets: typing.List[typing.Tuple[str, str, str, str, str]],
    # ---
    write_index: bool,
    # ---
    fuse_pid: int,
    fuse_signal_number: int,
):
    from .runner import is_inside
    if not is_inside:
        print("not in a runner")
        raise click.Abort()

    from . import monkey_patches
    monkey_patches.apply_all()

    competition_format = api.CompetitionFormat[competition_format]

    if competition_format == api.CompetitionFormat.TIMESERIES:
        split_key_type = api.SplitKeyType[split_key_type]

        if split_key_type == api.SplitKeyType.INTEGER:
            loop_key = int(loop_key)

    from .runner.cloud_executor import SandboxExecutor
    executor = SandboxExecutor(
        competition_name,
        competition_format,
        # ---
        x_path,
        y_path,
        y_raw_path,
        orthogonalization_data_path,
        data_directory_path,
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
        loop_key,
        embargo,
        number_of_features,
        gpu,
        # ---
        api.ColumnNames(
            id_column_name,
            moon_column_name,
            side_column_name or None,
            input_column_name or None,
            output_column_name or None,
            [
                api.TargetColumnNames(
                    0,
                    target_name,
                    side or None,
                    input or None,
                    output or None,
                    file_path or None,
                )
                for target_name, side, input, output, file_path in targets
            ]
        ),
        # ---
        write_index,
        # ---
        fuse_pid,
        fuse_signal_number,
    )

    try:
        executor.start()
    except SystemExit as error:
        if error.code is None or error.code == 0:
            print("[debug] exit was called without a code, forcing `2`")

        error.code = 2
        raise


@cli.group(name="organizer")
@click.argument('competition_name')
@click.pass_context
def organize_group(
    context: click.Context,
    competition_name: str,
):
    client = api.Client.from_env()

    try:
        competition = client.competitions.get(competition_name)
    except api.errors.CompetitionNameNotFoundException:
        print(f"competition {competition_name} not found", file=sys.stderr)
        raise click.Abort()
    except api.ApiException as error:
        utils.exit_via(error)

    context.obj = competition


@organize_group.group(name="test")
def organize_test_group():
    pass


@organize_test_group.group(name="leaderboard")
@click.option("--script-file", "script_file_path", type=click.Path(dir_okay=False, readable=True), required=False)
@click.option("--github-repository", default=constants.COMPETITIONS_REPOSITORY, required=False)
@click.option("--github-branch", default=constants.COMPETITIONS_BRANCH, required=False)
@click.pass_context
def leaderboard_group(
    context: click.Context,
    script_file_path: str,
    github_repository: str,
    github_branch: str,
):
    from . import custom

    competition: api.Competition = context.obj

    if script_file_path is None:
        loader = custom.GithubCodeLoader(
            competition.name,
            "leaderboard",
            repository=github_repository,
            branch=github_branch,
        )
    else:
        loader = custom.LocalCodeLoader(
            script_file_path,
        )

    context.obj = (competition, loader)


@leaderboard_group.command(name="rank")
@click.option("--scores-file", "score_file_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--shuffle", is_flag=True)
@click.pass_context
def leaderboard_rank(
    context: click.Context,
    score_file_path: str,
    shuffle,
):
    from . import custom

    competition, loader = typing.cast(
        typing.Tuple[
            api.Competition,
            custom.CodeLoader,
        ],
        context.obj
    )

    module = custom.LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    with open(score_file_path, "r") as fd:
        root = json.load(fd)
        if not isinstance(root, list):
            raise ValueError("root must be a list")

        projects = [
            custom.RankableProject.from_dict(item)
            for item in root
        ]

        if shuffle:
            random.shuffle(projects)

    try:
        metrics = competition.metrics.list()

        ranked_projects = module.rank(
            metrics,
            projects,
        )

        print(f"\n\nLeaderboard is ranked")

        used_metric_ids = list({
            metric.id
            for project in projects
            for metric in project.metrics
        })

        metric_name_by_id = {
            metric.id: metric.name
            for metric in metrics
            if metric.id in used_metric_ids
        }

        score_by_metric_id_by_project_id = {
            project.id: {
                metric.id: metric.score
                for metric in project.metrics
            }
            for project in projects
        }

        print(f"\nResults:")
        utils.ascii_table(
            (
                "Rank",
                "Reward Rank",
                "Project ID",
                *[
                    f"Metric: {metric_name_by_id[id]}"
                    for id in used_metric_ids
                ]
            ),
            [
                (
                    ranked_project.rank,
                    ranked_project.reward_rank,
                    ranked_project.id,
                    *(
                        score_by_metric_id_by_project_id[ranked_project.id].get(metric_id)
                        for metric_id in used_metric_ids
                    )
                )
                for ranked_project in ranked_projects
            ]
        )
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@leaderboard_group.command(name="compare")
@click.option("--prediction-file", "prediction_file_paths", type=(int, click.Path(exists=True, dir_okay=False)), multiple=True)
@click.pass_context
def leaderboard_compare(
    context: click.Context,
    prediction_file_paths: typing.List[typing.Tuple[int, str]],
):
    from . import custom

    competition, loader = typing.cast(
        typing.Tuple[
            api.Competition,
            custom.CodeLoader,
        ],
        context.obj
    )

    module = custom.LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    predictions = {}
    for prediction_id, prediction_file_path in prediction_file_paths:
        if prediction_id in predictions:
            print(f"prediction id {prediction_id} specified multiple time")
            raise click.Abort()

        predictions[prediction_id] = pandas.read_parquet(prediction_file_path)

    try:
        targets = competition.targets.list()

        similarities = module.compare(
            targets,
            predictions,
        )

        print(f"\n\nSimilarities have been compared")

        target_per_id = {
            target.id: target
            for target in targets
        }

        prediction_name_per_id = {
            id: os.path.splitext(path)[0]
            for id, path in prediction_file_paths
        }

        print(f"\nResults:")
        utils.ascii_table(
            (
                "Target Name",
                "Left",
                "Right",
                "Similarity"
            ),
            [
                (
                    target_per_id[similarity.target_id].name,
                    prediction_name_per_id[similarity.left_id],
                    prediction_name_per_id[similarity.right_id],
                    similarity.value,
                )
                for similarity in similarities
            ]
        )
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="scoring")
@click.option("--script-file", "script_file_path", type=click.Path(dir_okay=False, readable=True), required=False)
@click.option("--github-repository", default=constants.COMPETITIONS_REPOSITORY, required=False)
@click.option("--github-branch", default=constants.COMPETITIONS_BRANCH, required=False)
@click.pass_context
def scoring_group(
    context: click.Context,
    script_file_path: str,
    github_repository: str,
    github_branch: str,
):
    from . import custom

    competition: api.Competition = context.obj

    if script_file_path is None:
        loader = custom.GithubCodeLoader(
            competition.name,
            "scoring",
            repository=github_repository,
            branch=github_branch,
        )
    else:
        loader = custom.LocalCodeLoader(
            script_file_path,
        )

    context.obj = (competition, loader)


LOWER_PHASE_TYPES = list(map(lambda x: x.name, [
    api.PhaseType.SUBMISSION,
    api.PhaseType.OUT_OF_SAMPLE,
]))


@scoring_group.command(name="check")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-file", "prediction_file_path", type=click.Path(dir_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(LOWER_PHASE_TYPES), default=LOWER_PHASE_TYPES[0])
@click.pass_context
def scoring_check(
    context: click.Context,
    data_directory_path: str,
    prediction_file_path: str,
    phase_type_string: str,
):
    from . import custom

    competition, loader = typing.cast(
        typing.Tuple[
            api.Competition,
            custom.CodeLoader,
        ],
        context.obj
    )

    phase_type = api.PhaseType[phase_type_string]

    try:
        custom.scoring_check(
            custom.ScoringModule.load(loader),
            phase_type,
            competition.metrics.list(),
            utils.read(prediction_file_path),
            data_directory_path
        )

        print(f"\n\nPrediction is valid!")
    except custom.ParticipantVisibleError as error:
        print(f"\n\nPrediction is not valid: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction check function failed: {error}")

        traceback.print_exc()


@scoring_group.command(name="score")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-file", "prediction_file_path", type=click.Path(dir_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(LOWER_PHASE_TYPES), default=LOWER_PHASE_TYPES[0])
@click.pass_context
def scoring_score(
    context: click.Context,
    data_directory_path: str,
    prediction_file_path: str,
    phase_type_string: str,
):
    from . import custom

    competition, loader = typing.cast(
        typing.Tuple[
            api.Competition,
            custom.CodeLoader,
        ],
        context.obj
    )

    phase_type = api.PhaseType[phase_type_string]

    try:
        metrics = competition.metrics.list()
        results = custom.scoring_score(
            custom.ScoringModule.load(loader),
            phase_type,
            metrics,
            utils.read(prediction_file_path),
            data_directory_path,
        )

        metric_by_id = {
            metric.id: metric
            for metric in metrics
        }

        print(f"\n\nPrediction is scorable!")

        print(f"\nResults:")
        utils.ascii_table(
            ("Target", "Metric", "Score", "Details"),
            [
                (
                    metric_by_id[metric_id].target.name,
                    metric_by_id[metric_id].name,
                    str(scored_metric.value),
                    " ".join((
                        f"{detail.key}={detail.value}"
                        for detail in scored_metric.details
                    ))
                )
                for metric_id, scored_metric in results.items()
            ]
        )
    except custom.ParticipantVisibleError as error:
        print(f"\n\nPrediction is not scorable: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction score function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="submission")
@click.option("--script-file", "script_file_path", type=click.Path(dir_okay=False, readable=True), required=False)
@click.option("--github-repository", default=constants.COMPETITIONS_REPOSITORY, required=False)
@click.option("--github-branch", default=constants.COMPETITIONS_BRANCH, required=False)
@click.pass_context
def submission_group(
    context: click.Context,
    script_file_path: str,
    github_repository: str,
    github_branch: str,
):
    from . import custom

    competition: api.Competition = context.obj

    if script_file_path is None:
        loader = custom.GithubCodeLoader(
            competition.name,
            "submission",
            repository=github_repository,
            branch=github_branch,
        )
    else:
        loader = custom.LocalCodeLoader(
            script_file_path,
        )

    context.obj = (competition, loader)


@submission_group.command(name="check")
@click.option("--root-directory", "root_directory_path", type=click.Path(exists=True, file_okay=False), required=True)
@click.option("--model-directory", "model_directory_path", default=constants.DEFAULT_MODEL_DIRECTORY)
@click.pass_context
def submission_check(
    context: click.Context,
    root_directory_path: str,
    model_directory_path: str,
):
    from . import custom
    from .command.push import list_code_files, list_model_files

    _, loader = typing.cast(
        typing.Tuple[
            api.Competition,
            custom.CodeLoader,
        ],
        context.obj
    )

    module = custom.SubmissionModule.load(loader)
    if module is None:
        print(f"no custom submission check found")
        raise click.Abort()

    def from_local(path: str, name: str):
        _, extension = os.path.splitext(path)
        can_load = extension in constants.TEXT_FILE_EXTENSIONS

        return custom.File(
            name,
            uri=path if can_load else None,
            size=os.path.getsize(path),
        )

    submission_files = [
        from_local(path, name)
        for path, name in list_code_files(root_directory_path, model_directory_path)
    ]

    model_files = [
        from_local(path, name)
        for path, name in list_model_files(root_directory_path, model_directory_path)
    ]

    try:
        custom.submission_check(
            custom.SubmissionModule.load(loader),
            submission_files,
            model_files,
        )

        print(f"\n\nSubmission is valid!")
    except custom.ParticipantVisibleError as error:
        print(f"\n\nSubmission is not valid: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nSubmission check function failed: {error}")

        traceback.print_exc()


if __name__ == '__main__':
    cli()
