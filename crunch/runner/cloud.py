import json
import os
import random
import re
import signal
import stat
import string
import subprocess
import sys
import time
from typing import Any, Callable, Dict, Generator, Iterable, List, Literal, Optional, Tuple, Union, cast
from urllib.parse import urljoin

import requests

import crunch.store as store
import requirements as requirements_parser
from crunch.api import Client, Competition, CompetitionFormat, DataReleaseSplitGroup, Language, ModelTooBigException, PhaseType, PredictionTooBigException, RunnerRun, Upload
from crunch.downloader import prepare_all, save_all
from crunch.runner.runner import Runner
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import RunnerContext
from crunch.unstructured import GithubCodeLoader, LocalCodeLoader, RunnerModule, deduce_code_loader
from crunch.utils import download

UploadedFiles = Dict[str, Upload]
LastModifications = Dict[str, int]

PACKAGES_WITH_PRIORITY = [
    "torch"
]

CONFLICTING_GPU_PACKAGES = [
    "nvidia_cublas_cu11"
]

"""
Write permissions for current user, current group and others.
"""
S_IWALL = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH

"""
group + other  = (null)
user (runner)  = read
"""
CHMOD_RESET = "go=,u=r"

"""
SIGCONT is the only allowed signal.
SIGUSR1 would not be transmitted because of the privileges drop of the sandbox.
"""
FUSE_SIGNAL: signal.Signals = signal.SIGCONT  # pyright: ignore[reportUnknownVariableType, reportAttributeAccessIssue, reportUnknownMemberType]


R_SITE_LIBRARY_PATHS = [
    "/usr/lib/R/site-library",
    "/usr/local/lib/R/site-library"
]


class CloudRunner(Runner):

    has_model: bool

    def __init__(
        self,
        competition: Competition,
        run: RunnerRun,
        client: Client,
        # ---
        context_directory: str,
        scoring_directory: str,
        state_file: str,
        venv_directory: str,
        data_directory: str,
        code_directory: str,
        main_file: str,
        # ---
        requirements_txt_path: str,
        requirements_r_txt_path: str,
        model_directory_path: str,
        prediction_directory_path: str,
        trace_path: str,
        exit_file_path: str,
        # ---
        log_secret: str,
        train_frequency: int,
        force_first_train: bool,
        determinism_check_enabled: bool,
        gpu: bool,
        phase_type: PhaseType,
        chain_height: int,
        crunch_cli_commit_hash: str,
        # ---
        max_retry: int,
        retry_seconds: int
    ):
        super().__init__(
            competition_format=competition.format,
            prediction_directory_path=prediction_directory_path,
            determinism_check_enabled=determinism_check_enabled,
        )

        self.competition = competition
        self.run = run
        self.client = client

        self.context_directory = context_directory
        self.scoring_directory = scoring_directory
        self.state_file = state_file
        self.venv_directory = venv_directory
        self.data_directory = data_directory
        self.code_directory = code_directory
        self.main_file = main_file
        self.runner_dot_py_file_path = None

        self.requirements_txt_path = requirements_txt_path
        self.requirements_r_txt_path = requirements_r_txt_path
        self.model_directory_path = model_directory_path
        self.prediction_directory_path = prediction_directory_path
        self.trace_path = trace_path

        self.exit_file_path = exit_file_path
        self.exit_content = None

        self.log_secret = log_secret
        self.train_frequency = train_frequency
        self.force_first_train = force_first_train
        self.gpu = gpu
        self.phase_type = phase_type
        self.chain_height = chain_height
        self.crunch_cli_commit_hash = crunch_cli_commit_hash

        self.max_retry = max_retry
        self.retry_seconds = retry_seconds

    def start(self):
        self.report_current("starting")

        super().start()

        self.report_current("ending")

    def initialize(self):
        if self.competition_format != CompetitionFormat.UNSTRUCTURED:
            raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")

        if self.log("downloading runner..."):
            self.report_current("download runner")

            loader = deduce_code_loader(
                competition_name=self.competition.name,
                file_name="runner",
            )

            if isinstance(loader, GithubCodeLoader):
                source = loader.source

                self.runner_dot_py_file_path = os.path.join(self.scoring_directory, "runner.py")
                with open(self.runner_dot_py_file_path, "w") as fd:
                    fd.write(source)

                self.bash2(["chmod", "a+r", self.runner_dot_py_file_path])

                loader = LocalCodeLoader(path=self.runner_dot_py_file_path)
            elif isinstance(loader, LocalCodeLoader):  # type: ignore
                self.runner_dot_py_file_path = os.path.realpath(loader.path)

            self.runner_module = RunnerModule.load(loader)
            if self.runner_module is None:
                raise RuntimeError("no runner is available for this competition")

        if self.log("downloading code..."):
            self.report_current("download code")

            _download_files(
                file_urls=self.run.code,
                directory_path=self.code_directory,
                print=self.log,  # type: ignore
            )

        if os.path.exists(self.requirements_r_txt_path):
            self.report_current("install r requirements")

            self.log("installing r...")
            self.bash("r-apt", ["apt-get", "-qq", "update"])
            self.bash("r-apt", ["apt-get", "-qq", "install", "r-base"])

            with open(self.requirements_r_txt_path, "r") as fd:
                requirements = requirements_parser.parse(fd.read())

            apt_names = [
                f"r-cran-{requirement.name}"
                for requirement in requirements
            ]

            if len(apt_names):
                self.log("installing r cran packages...")
                self.bash("r-apt", ["apt-get", "-qq", "install", *apt_names])
            else:
                self.log("no r cran packages to install")

            # remove warning about empty directory
            user_site_library_paths = [
                path
                for path in R_SITE_LIBRARY_PATHS
                if os.path.exists(path) and len(os.listdir(path)) == 0
            ]

            if len(user_site_library_paths):
                self.bash2(["rm", "-r", *user_site_library_paths])

        if self.log("installing python requirements..."):
            if os.path.exists(self.requirements_txt_path):
                self.report_current("install python requirements")

                constraints_txt_path = self._download_constraints(Language.PYTHON)
                constraints_args: Iterable[str] = ("-c", constraints_txt_path) if constraints_txt_path else []

                priority_packages: List[str] = []
                with open(self.requirements_txt_path) as fd:
                    for line in fd:
                        line = line.strip()

                        package = re.search(r"^([\w-]+)", line, re.MULTILINE)
                        if package is None:
                            continue

                        package = package.group(1)
                        if package in PACKAGES_WITH_PRIORITY:
                            priority_packages.append(line)

                if priority_packages:
                    self.pip([
                        *priority_packages,
                        *constraints_args
                    ])

                self.pip([
                    *(["--no-build-isolation"] if priority_packages else []),
                    "-r", self.requirements_txt_path,
                    *constraints_args
                ])
            else:
                self.log("no requirements.txt found")

        if self.log("installing crunch-cli..."):
            self.report_current("install crunch-cli")

            self.pip([
                "--upgrade",
                "--force-reinstall",
                "--no-deps",
                f"git+https://github.com/crunchdao/crunch-cli@{self.crunch_cli_commit_hash}"
            ])

        if self.gpu and self.log("fixing gpu requirements..."):
            self.report_current("fix gpu requirements")

            self.bash("pip", [
                "pip3", "uninstall",
                "--root-user-action", "ignore",
                "--disable-pip-version-check",
                "--no-input",
                "--no-color",
                "--yes",
                *(["--quiet"] * 2),
                *CONFLICTING_GPU_PACKAGES
            ], self.venv_env)

        if self.log("downloading data..."):
            self.report_current("download data")

            self.prepare_data()

            self.initialize_state()

        if self.log("downloading model..."):
            self.report_current("download model")

            self.bash2(["mkdir", "-p", self.model_directory_path])

            file_urls = self.run.model
            self.has_model = len(file_urls) != 0

            self.pre_model_files_modification = _download_files(
                file_urls=file_urls,
                directory_path=self.model_directory_path,
                print=self.log,  # type: ignore
            )

            self.bash2(["chmod", "-R", "o+rw", self.model_directory_path])

        if self.log("prepare prediction directory..."):
            self.bash2(["mkdir", "-p", self.prediction_directory_path])
            self.delete_content(self.prediction_directory_path)
            self.bash2(["chmod", "-R", "o+rw", self.prediction_directory_path])

        return (
            self.keys,
            self.has_model
        )

    def _download_constraints(self, language: Language) -> Optional[str]:
        lower = language.name.lower()
        txt_path = os.path.join(self.context_directory, f"{lower}-constraints.txt")

        response = requests.get(urljoin(store.api_base_url, f"/v1/libraries/{lower}/~/constraints"))
        if not response.ok:
            self.log(f"failed to download constraints: {response.status_code}: {response.text}", error=True)
            return

        with open(txt_path, "w") as fd:
            fd.write(response.text)

        return txt_path

    def start_unstructured(self):
        self.create_trace_file()

        if self.runner_module is None:
            raise RuntimeError("no runner is available for this competition")

        context = CloudRunnerContext(self)

        return self.runner_module.run(
            context=context,
            data_directory_path=self.data_directory,
            model_directory_path=self.model_directory_path,
            prediction_directory_path=self.prediction_directory_path,
        )

    def finalize(self):
        self.report_current("upload result")
        self.log("uploading result...")

        self.upload_results()

    def upload_results(self):
        prediction_uploads: UploadedFiles = {}
        model_uploads: UploadedFiles = {}

        # TODO Use decorator instead
        try:
            for retry in range(0, self.max_retry + 1):
                if retry != 0:
                    retry_in = self.retry_seconds * retry

                    self.report_current("wait retry")
                    self.log(f"retrying in {retry_in} second(s)")
                    time.sleep(retry_in)
                    self.report_current(f"upload result try #{retry}")

                try:
                    self._upload_prediction_files(prediction_uploads)
                    has_model_changed = self._upload_model_files(model_uploads)

                    self.run.submit_result(
                        use_initial_model=not has_model_changed,
                        deterministic=self.deterministic,
                        prediction_files={
                            name: upload.id
                            for name, upload in prediction_uploads.items()
                        },
                        model_files={
                            name: upload.id
                            for name, upload in model_uploads.items()
                        },
                    )

                    self.log("result submitted")
                    break
                except (ModelTooBigException, PredictionTooBigException) as exception:
                    self.log(f"failed to submit result: {exception}: {exception.size}/{exception.maximum_size} bytes", error=True)
                    exit(1)
                except Exception as exception:
                    self.log(f"failed to submit result: {exception}", error=True)

                    if isinstance(exception, requests.HTTPError):
                        self.log(exception.response.text, error=True)
            else:
                self.log("max retry reached", error=True)
                exit(1)
        finally:
            self._delete_uploads(prediction_uploads)
            self._delete_uploads(model_uploads)

    def _upload_prediction_files(
        self,
        uploads: UploadedFiles,
    ):
        """
        Uploads the prediction files.
        """

        _upload_files(
            category="prediction",
            directory_path=self.prediction_directory_path,
            uploads=uploads,
            client=self.client,
            log=self.log,  # type: ignore
        )

    def _upload_model_files(
        self,
        uploads: UploadedFiles,
    ):
        """
        Uploads the models files.

        Returns:
            True if the model files have changed, False otherwise.
        """

        return _upload_files(
            category="model",
            directory_path=self.model_directory_path,
            uploads=uploads,
            client=self.client,
            log=self.log,  # type: ignore
            pre_modifications=self.pre_model_files_modification,
        )

    def _delete_uploads(
        self,
        uploads: UploadedFiles,
    ):
        """
        Delete the uploads.
        """

        for upload in uploads.values():
            try:
                upload.delete()
            except Exception as exception:
                self.log(f"[debug] failed to delete upload {upload.id}: {exception}", error=True)

    def teardown(self):
        self.report_current(f"shutting down")

    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ):
        file = sys.stderr if error else sys.stdout
        prefix = f"<runner/{file.name[4:-1]}>"

        if self.log_secret:
            print(f"[{self.log_secret}] {prefix} {message}", file=file)
        else:
            print(f"{prefix} {message}", file=file)

        return True

    def create_trace_file(self):
        self.bash2(["touch", self.trace_path])
        self.bash2(["chmod", "o+w", self.trace_path])

    def initialize_state(self):
        state: KwargsLike = {
            "splits": [
                {
                    "key": split.key,
                    "group": split.group.name,
                }
                for split in self.splits
            ],
        }

        with open(self.state_file, "w") as fd:
            json.dump(state, fd)

        self.bash2(["chmod", "a+r", self.state_file])

    def do_bash(
        self,
        arguments: List[str],
        env_vars: Optional[Dict[str, Optional[str]]] = None
    ):
        env = None
        if env_vars:
            env = os.environ.copy()
            for key, value in env_vars.items():
                if value is not None:
                    env[key] = value
                elif key in env:
                    del env[key]

        self.log(f"executing command: {' '.join(arguments)}")
        process = subprocess.Popen(
            arguments,
            env=env,
            cwd=self.code_directory,
        )

        code = process.wait()
        if code != 0:
            self.log(f"command not exited correctly: {code}", error=True)
            exit(code)

    def bash(
        self,
        prefix: str,
        arguments: List[str],
        env: Optional[Dict[str, Optional[str]]] = None
    ):
        arguments = [
            "prefix",
            prefix,
            "--",
            *arguments
        ]

        self.do_bash(arguments, env)

    def bash2(
        self,
        arguments: List[str],
    ):
        self.bash(arguments[0], arguments)

    def pip(
        self,
        arguments: List[str],
    ):
        self.bash(
            "pip",
            [
                "pip3", "install",
                "--root-user-action", "ignore",
                "--disable-pip-version-check",
                "--no-input",
                "--no-color",
                "--progress-bar", "off",
                "--resume-retries", "5",  # https://ichard26.github.io/blog/2025/04/whats-new-in-pip-25.1/#resumable-downloads
                *arguments
            ],
            self.venv_env
        )

    def recursive_bash(
        self,
        directory_path: str,
        commands: List[str],
    ):
        self.bash2([
            "find",
            directory_path,
            "-mindepth", "1",
            "-maxdepth", "1",
            "-exec", *commands, ";"
        ])

    def delete_content(
        self,
        directory_path: str,
    ):
        """
        Deletes all content in the given directory.
        """

        self.recursive_bash(
            directory_path,
            ["rm", "-rf", "{}"]
        )

    def sandbox(
        self,
        train: bool,
        loop_key: Union[int, str],
        parameters: KwargsLike = {},
    ) -> None:
        try:
            self._prepare_exit()

            self.bash2(["chmod", "-R", "a+r", self.data_directory])
            self._install_permission_fuse()

            options: KwargsLike = {
                "competition-name": self.competition.name,
                "competition-format": self.competition.format.name,
                "split-key-type": self.competition.split_key_type.name,
                # ---
                "data-directory": self.data_directory,
                # ---
                "main-file": self.main_file,
                "code-directory": self.code_directory,
                "model-directory": self.model_directory_path,
                "prediction-directory": self.prediction_directory_path,
                "trace": self.trace_path,
                "state-file": self.state_file,
                "ping-url": [
                    urljoin(
                        store.api_base_url,
                        "/v1/runner/ping"
                    ),
                    "https://1.1.1.1",  # CloudFlare
                ],
                # ---
                "train": train,
                "loop-key": loop_key,
                "embargo": self.embargo,
                "number-of-features": self.number_of_features,
                "gpu": self.gpu,
                # ---
                "fuse-pid": os.getpid(),
                "fuse-signal-number": FUSE_SIGNAL.value,
                "exit-file": self.exit_file_path,
                "exit-content": self.exit_content,
                # ---
                "runner-py-file": self.runner_dot_py_file_path,
                "parameters": json.dumps(parameters),
            }

            # TODO move to a dedicated function
            args: List[str] = []

            def append_value(value: Any):
                if isinstance(value, tuple):
                    for x in value:  # pyright: ignore[reportUnknownVariableType]
                        args.append(str(x))  # pyright: ignore[reportUnknownArgumentType]
                else:
                    args.append(str(value))

            for key, value in options.items():
                if value is None:
                    continue

                if isinstance(value, bool):
                    value = str(value).lower()

                if isinstance(value, list):
                    for item in value:  # pyright: ignore[reportUnknownVariableType]
                        args.append(f"--{key}")
                        append_value(item)
                else:
                    args.append(f"--{key}")
                    append_value(value)

            self.do_bash(
                [
                    "sandbox",
                    "--verbose",
                    "--chown-directory", self.model_directory_path,
                    "--filter-non-unix-socket-syscall",
                    "--",
                    "prefix", "user-code",
                    "--",
                    "python3", "-u",
                    "-m", "crunch", "runner", "cloud-executor",
                    *args
                ],
                {
                    **self.venv_env,
                    "CRUNCH_AUTO_MONKEY_PATCH": "true",
                }
            )

            self._validate_exit()
        except SystemExit:
            self.report_trace(loop_key)  # pyright: ignore[reportArgumentType]
            raise

    def _prepare_exit(self):
        if not os.path.exists(self.exit_file_path):
            self.bash2(["touch", self.exit_file_path])

        if (os.stat(self.exit_file_path).st_mode & S_IWALL) != S_IWALL:
            self.bash2(["chmod", "a+w", self.exit_file_path])

        # truncate
        open(self.exit_file_path, "w").close()

        self.exit_content = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))

    def _validate_exit(self):
        expected_content = cast(str, self.exit_content)
        self.exit_content = None

        with open(self.exit_file_path) as fd:
            got_content = fd.read()

        if got_content != expected_content:
            self.log(f"[debug] failed exit check - expected=`{expected_content}` got=`{got_content[:len(expected_content) * 2]}`", error=True)
            raise RuntimeError("user code exited prematurely")

    def _install_permission_fuse(self):
        def call_chmod(mode: str):
            self.recursive_bash(
                self.data_directory,
                ["chmod", mode, "-R", "{}"],
            )

        call_chmod("o+r")

        def on_signal(signum: int, stack: Any):
            signal.signal(FUSE_SIGNAL, signal.SIG_DFL)

            call_chmod(CHMOD_RESET)

            self.log("[debug] fuse triggered")

        signal.signal(FUSE_SIGNAL, on_signal)

    @property
    def venv_env(self) -> Dict[str, Optional[str]]:
        venv_bin = os.path.join(self.venv_directory, "bin")

        return {
            "VIRTUAL_ENV": self.venv_directory,
            "PATH": f"{venv_bin}:{os.environ['PATH']}",
            "PYTHONHOME": None,
            "VIRTUAL_ENV_PROMPT": "(env) ",
        }

    def prepare_data(self):
        data_release = self.run.data

        self.embargo = data_release.embargo
        self.number_of_features = data_release.number_of_features
        self.splits = data_release.splits
        data_files = data_release.data_files

        self.keys = sorted([
            split.key
            for split in self.splits
            if split.group == DataReleaseSplitGroup.TEST
        ])

        save_all(
            prepare_all(
                self.data_directory,
                data_files,
            ),
            False,
            print=self.log,  # type: ignore
            progress_bar=False,
        )

    def report_current(
        self,
        work: str,
        moon: Optional[int] = None,
    ):
        try:
            self.run.report_current(work, moon)
        except BaseException as ignored:
            self.log(
                f"ignored exception when reporting current: {type(ignored).__name__}({ignored})",
                error=True,
            )

    def report_trace(
        self,
        moon: Optional[int] = None,
    ):
        try:
            content = "<no trace>"
            with open(self.trace_path) as fd:
                content = fd.read()

            self.run.report_trace(content, moon)

            self.log("trace reported")
        except BaseException as ignored:
            self.log(
                f"ignored exception when reporting trace: {type(ignored).__name__}({ignored})",
                error=True
            )


def _download_files(
    *,
    file_urls: Dict[str, str],
    directory_path: str,
    print: Callable[[str], None],
) -> LastModifications:
    pre_modifications: LastModifications = {}

    for relative_path, url in file_urls.items():
        path = os.path.join(directory_path, relative_path)
        download(
            url,
            path,
            print=print,
            progress_bar=False,
        )

        stat = os.stat(path)
        pre_modifications[relative_path] = stat.st_mtime_ns

    return pre_modifications


def _find_files(
    directory_path: str,
) -> Generator[Tuple[str, str], None, None]:
    """
    Recursively finds all files in the given directory and yields their absolute paths and relative names.
    """

    root_length = len(directory_path) + 1
    for root, _, filenames in os.walk(directory_path):
        relative = root[root_length:]

        for filename in filenames:
            file_path = os.path.join(root, filename)
            file_name = os.path.join(relative, filename)

            yield file_path, file_name


def _upload_files(
    *,
    category: str,
    directory_path: str,
    uploads: UploadedFiles,
    client: Client,
    log: Callable[[str], None],
    pre_modifications: Optional[LastModifications] = None,
) -> bool:
    """
    Uploads files from the given directory.
    If a files is already uploaded, it will be reused.

    Returns:
        True if the files have changed, False otherwise.
        Always True if `pre_modifications` is None.
    """

    files: List[Tuple[str, str]] = []
    total_size = 0
    post_modifications: LastModifications = {}

    for file_path, file_name in _find_files(directory_path):
        stat = os.stat(file_path)
        file_size = stat.st_size

        log(f"{category}: found file name=`{file_name}` size={file_size}")

        files.append((file_path, file_name))
        post_modifications[file_name] = stat.st_mtime_ns
        total_size += file_size

    if pre_modifications is not None:
        has_changed = pre_modifications != post_modifications
        log(f"{category}: done walking files.len={len(files)} total_size={total_size} has_changed={has_changed}")

        if not has_changed:
            return False
    else:
        log(f"{category}: done walking files.len={len(files)} total_size={total_size}")

    for file_path, file_name in files:
        if file_name in uploads:
            log(f"{category}: reusing upload name=`{file_name}`")
            continue

        log(f"{category}: uploading name=`{file_name}`")
        uploads[file_name] = client.uploads.send_from_file(
            path=file_path,
            name=file_name,
            max_retry=3
        )

    return True


class CloudRunnerContext(RunnerContext):

    def __init__(
        self,
        runner: CloudRunner,
    ):
        self.runner = runner

    @property
    def train_frequency(self):
        return self.runner.train_frequency

    @property
    def force_first_train(self):
        return self.runner.force_first_train

    @property
    def is_local(self):
        return False

    @property
    def is_submission_phase(self):
        return self.runner.phase_type == PhaseType.SUBMISSION

    @property
    def chain_height(self):
        return self.runner.chain_height

    @property
    def has_model(self):
        return self.runner.has_model

    @property
    def is_determinism_check_enabled(self):
        return self.runner.determinism_check_enabled

    def report_determinism(self, deterministic: bool):
        self.runner.deterministic = deterministic

    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ) -> Literal[True]:
        return self.runner.log(
            message,
            important=important,
            error=error,
        )

    def execute(
        self,
        *,
        command: str,
        parameters: Optional[KwargsLike] = None,
    ) -> None:
        self.log(f"executing - command={command}")

        self.runner.sandbox(
            self.runner.force_first_train,
            command,
            parameters=parameters or {},
        )
