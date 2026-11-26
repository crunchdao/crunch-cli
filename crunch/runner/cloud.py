import graphlib
import json
import logging
import os
import random
import shutil
import signal
import stat
import string
import subprocess
import sys
from datetime import timedelta
from threading import Lock, Timer
from time import sleep
from typing import Any, Generator, TypeAlias
from urllib.parse import urljoin
from uuid import uuid4

import requests
from crunch_convert import RequirementLanguage
from crunch_convert.requirements_txt import parse_from_file
from retry import retry

import crunch.store as store
from crunch.api import Client, Competition, InvalidRunTokenException, Language, LibraryInstallation, ModelTooBigException, PhaseType, PredictionTooBigException, RunnerRun, Upload
from crunch.download import download_data_files, download_file, download_files
from crunch.runner.installation import InstallationBatch, plan_installation
from crunch.runner.runner import Runner
from crunch.runner.tracing import GpuPresence, RemoteTraceExporter, RunnerTracer, to_execute_span_attributes
from crunch.runner.types import KwargsLike
from crunch.runner.unstructured import DEFAULT_LOG_LIMITS, RunnerContext
from crunch.unstructured import LocalCodeLoader, RunnerModule, deduce_code_loader

UploadedFiles: TypeAlias = dict[str, Upload]
LastModifications: TypeAlias = dict[str, int]

CONFLICTING_GPU_PACKAGES = [
    "nvidia_cublas_cu11"
]

"""
Read permissions for current user, current group and others.
"""
MODE_READ_ALL = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

"""
Write permissions for current user, current group and others.
"""
MODE_WRITE_ALL = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH

"""
Execute/traverse permissions for current user, current group and others.
"""
MODE_EXECUTE_ALL = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH

"""
Read and execute/traverse permissions for current user, current group and others.
"""
MODE_READ_AND_EXECUTE_ALL = MODE_READ_ALL | MODE_EXECUTE_ALL

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

"""
All permissions for the runner, only execute/traverse for the others: they cannot list, add, rename or delete anything inside.
"""
MODE_TRAVERSE_ONLY_FOR_OTHERS = stat.S_IRWXU | MODE_EXECUTE_ALL

"""
Read and write permissions for the runner, only write for the others.
"""
MODE_WRITE_ONLY_FOR_OTHERS = stat.S_IRUSR | MODE_WRITE_ALL

"""
Removed from the runner's environment when it starts: the sandbox refuses to run if one is still there.
"""
SECRET_ENVIRONMENT_VARIABLE_NAMES = [
    "RUN_ID",
    "RUN_TOKEN",
    "LOG_SECRET",
]

"""
The exit code of the runner when the run takes longer than its timeout.
"""
TIMEOUT_EXIT_CODE = 124

"""
The size of the trace that the runner reads, in bytes: its end, which holds the exception.
"""
MAX_TRACE_SIZE = 64 * 1024


R_SITE_LIBRARY_PATHS = [
    "/usr/lib/R/site-library",
    "/usr/local/lib/R/site-library"
]


class CloudRunner(Runner):

    has_model: bool
    sandbox_path: str
    prefix_path: str

    def __init__(
        self,
        competition: Competition,
        run: RunnerRun,
        client: Client,
        # ---
        context_directory: str,
        scoring_directory: str,
        venv_directory: str,
        data_directory: str,
        code_directory: str,
        main_file: str,
        # ---
        model_directory_path: str,
        prediction_directory_path: str,
        # ---
        log_secret: str,
        train_frequency: int,
        force_first_train: bool,
        determinism_check_enabled: bool,
        gpu: bool,
        phase_type: PhaseType,
        chain_height: int,
        crunch_cli_commit_hash: str,
        timeout: int | None,
        # ---
        max_retry: int,
        retry_seconds: int
    ):
        logger = _create_logger(log_secret)

        super().__init__(
            competition_format=competition.format,
            tracer=RunnerTracer(
                RemoteTraceExporter(run),
                logger=logger,
                gpu_presence=GpuPresence.AVAILABLE if gpu else GpuPresence.UNAVAILABLE,
                metrics_delay=60,
            ),
            logger=logger,
            determinism_check_enabled=determinism_check_enabled,
        )

        self.competition = competition
        self.run = run
        self.client = client

        self.context_directory = context_directory
        self.scoring_directory = scoring_directory
        self.venv_directory = venv_directory

        self.sandboxes_directory_path = os.path.join(context_directory, "sandbox")

        self.data_directory = data_directory
        self.code_directory = code_directory
        self.main_file = main_file
        self.runner_dot_py_file_path = None

        self.model_directory_path = model_directory_path
        self.prediction_directory_path = prediction_directory_path

        self.log_secret = log_secret
        self.train_frequency = train_frequency
        self.force_first_train = force_first_train
        self.gpu = gpu
        self.phase_type = phase_type
        self.chain_height = chain_height
        self.crunch_cli_commit_hash = crunch_cli_commit_hash
        self.timeout = timeout

        self.max_retry = max_retry
        self.retry_seconds = retry_seconds

        self._error_reported_fuse = Lock()

        self._active_sandbox_count = 0
        self._active_sandbox_count_lock = Lock()

    def setup(self):
        self.sandbox_path = self._locate("sandbox")
        self.prefix_path = self._locate("prefix")

        # without a handler, PID 1 ignores SIGTERM
        signal.signal(signal.SIGTERM, lambda signum, frame: os._exit(128 + signum))

        timeout = self.timeout
        if timeout is not None:
            def exit_on_timeout():
                self.logger.error(f"timeout of {timeout} seconds reached")
                os._exit(TIMEOUT_EXIT_CODE)

            timer = Timer(timeout, exit_on_timeout)
            timer.daemon = True
            timer.start()

    def _locate(self, command: str) -> str:
        path = shutil.which(command)
        if path is None:
            self.logger.error(f"command not found: {command}")
            sys.exit(1)

        return path

    def initialize(self):
        os.makedirs(self.data_directory, exist_ok=True)
        os.makedirs(self.scoring_directory, exist_ok=True)
        os.makedirs(self.code_directory, exist_ok=True)
        os.makedirs(self.sandboxes_directory_path, exist_ok=True)

        os.chmod(self.data_directory, MODE_READ_AND_EXECUTE_ALL)

        os.chmod(self.sandboxes_directory_path, MODE_TRAVERSE_ONLY_FOR_OTHERS)

        self._download_runner()

        try:
            code_file_urls = self.run.code
            model_file_urls = self.run.model
        except InvalidRunTokenException:
            self.logger.error("invalid run token")
            sys.exit(1)

        if "install r requirements":
            requirements_r_txt_name = "requirements.r.txt"
            requirements_r_txt_url = code_file_urls.pop(requirements_r_txt_name, None)
            if requirements_r_txt_url is not None:
                requirements_r_txt_path = os.path.join(self.context_directory, requirements_r_txt_name)

                download_file(
                    requirements_r_txt_url,
                    requirements_r_txt_path,
                    logger=self.logger,
                    verbose=True,
                    progress_bar=False,
                )

                with self._span("installing r requirements"):
                    self._install_r_requirements(requirements_r_txt_path)

        if "install python requirements":
            requirements_txt_name = "requirements.txt"
            requirements_txt_url = code_file_urls.pop(requirements_txt_name, None)
            if requirements_txt_url is not None:
                requirements_txt_path = os.path.join(self.context_directory, requirements_txt_name)

                download_file(
                    requirements_txt_url,
                    requirements_txt_path,
                    logger=self.logger,
                    verbose=True,
                    progress_bar=False,
                )

                with self._span("installing python requirements"):
                    self._install_python_requirements(requirements_txt_path)
            else:
                self.logger.info("no requirements.txt found")

        with self._span("installing crunch-cli"):
            self._install_crunch_cli()

        if self.gpu:
            with self._span("fixing gpu requirements"):
                self._fix_gpu_requirements()

        with self._span("downloading data"):
            self.prepare_data()

        with self._span("downloading code"):
            _download_files(
                file_urls=code_file_urls,
                directory_path=self.code_directory,
                logger=self.logger,
            )

        with self._span("downloading model"):
            os.makedirs(self.model_directory_path, exist_ok=True)

            self.has_model = len(model_file_urls) != 0

            self.pre_model_files_modification = _download_files(
                file_urls=model_file_urls,
                directory_path=self.model_directory_path,
                logger=self.logger,
            )

            self.bash2(["chmod", "-R", "o+rw", self.model_directory_path])

            # no user code ran yet: no link of the user can be there
            self.bash("sandbox", [self.sandbox_path, "grant", "--directory", self.model_directory_path])

        if "prepare prediction directory...":
            os.makedirs(self.prediction_directory_path, exist_ok=True)
            self.delete_content(self.prediction_directory_path)
            self.bash2(["chmod", "-R", "o+rw", self.prediction_directory_path])

    def start_unstructured(self):
        if self.runner_module is None:
            raise RuntimeError("no runner is available for this competition")

        context = CloudRunnerContext(self)

        with self._span("executing runner script"):
            self.runner_module.run(
                context=context,
                data_directory_path=self.data_directory,
                model_directory_path=self.model_directory_path,
                prediction_directory_path=self.prediction_directory_path,
                tracer=self.tracer,
            )

    def finalize(self):
        with self._span("uploading result"):
            self._upload_results()

    def _download_runner(self):
        loader = deduce_code_loader(
            competition_name=self.competition.name,
            file_name="runner",
        )

        if isinstance(loader, LocalCodeLoader):
            self.runner_dot_py_file_path = os.path.realpath(loader.path)
            self.logger.info(f"loading runner script from: {self.runner_dot_py_file_path}")
        else:
            source_code = loader.load()

            self.runner_dot_py_file_path = os.path.join(self.scoring_directory, "runner.py")
            self.logger.info(f"loading runner script from: {self.runner_dot_py_file_path} (downloaded from {source_code.location})")

            with open(self.runner_dot_py_file_path, "w") as fd:
                fd.write(source_code.content)

            os.chmod(self.runner_dot_py_file_path, MODE_READ_ALL)

            loader = LocalCodeLoader(path=self.runner_dot_py_file_path)

        self.runner_module = RunnerModule.load(loader)
        if self.runner_module is None:
            raise RuntimeError("no runner is available for this competition")

    def _install_r_requirements(self, requirements_r_txt_path: str):
        self.bash("r-apt", ["apt-get", "-qq", "update"])
        self.bash("r-apt", ["apt-get", "-qq", "install", "r-base"])

        with open(requirements_r_txt_path, "r") as fd:
            requirements = parse_from_file(
                file_content=fd.read(),
                language=RequirementLanguage.R,
            )

        apt_names = [
            f"r-cran-{requirement.name}"
            for requirement in requirements
        ]

        if len(apt_names):
            self.logger.info("installing r cran packages...")
            self.bash("r-apt", ["apt-get", "-qq", "install", *apt_names])
        else:
            self.logger.info("no r cran packages to install")

        # remove warning about empty directory
        user_site_library_paths = [
            path
            for path in R_SITE_LIBRARY_PATHS
            if os.path.exists(path) and len(os.listdir(path)) == 0
        ]

        if len(user_site_library_paths):
            self.bash2(["rm", "-r", *user_site_library_paths])

    def _install_python_requirements(self, requirements_txt_path: str):
        installation = self._download_installation(Language.PYTHON)

        constraints_txt_path = os.path.join(self.context_directory, "python-constraints.txt")
        with open(constraints_txt_path, "w") as fd:
            fd.writelines(f"{line}\n" for line in installation.constraints)

        constraints_args = ["-c", constraints_txt_path]

        with open(requirements_txt_path) as fd:
            requirement_lines = fd.read().splitlines()

        try:
            batches = plan_installation(requirement_lines, installation.build_dependencies)
        except graphlib.CycleError as error:
            self.logger.error(f"build dependencies are cyclic, ignoring them: {error.args[1]}")
            batches = []

        if batches:
            self.logger.info(f"some libraries need others to be built, installing them in {len(batches)} steps first")

        for index, batch in enumerate(batches, start=1):
            self._log_installation_batch(batch, index, len(batches))

            self.pip([
                *([] if batch.build_isolation else ["--no-build-isolation"]),
                *batch.lines,
                *constraints_args,
            ])

        self.pip([
            "-r", requirements_txt_path,
            *constraints_args,
        ])

    def _log_installation_batch(self, batch: InstallationBatch, index: int, count: int):
        names = ", ".join(
            f"{name} (needs {', '.join(batch.needs[name])})" if name in batch.needs else name
            for name in batch.names
        )

        self.logger.info(f"step {index}/{count}: {names}")

    def _download_installation(self, language: Language) -> LibraryInstallation:
        @retry(tries=3, delay=2, backoff=2, logger=None)
        def download():
            return self.client.libraries.get_installation(language)

        try:
            return download()
        except Exception as error:
            self.logger.error(f"failed to download the installation: {error}")
            sys.exit(1)

    def _install_crunch_cli(self):
        self.pip([
            "--upgrade",
            "--force-reinstall",
            "--no-deps",
            f"https://codeload.github.com/crunchdao/crunch-cli/zip/{self.crunch_cli_commit_hash}"
        ])

    def _fix_gpu_requirements(self):
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

    def _upload_results(self):
        prediction_uploads: UploadedFiles = {}
        model_uploads: UploadedFiles = {}

        # TODO Use decorator instead
        try:
            for attempt in range(0, self.max_retry + 1):
                if attempt != 0:
                    retry_in = self.retry_seconds * attempt

                    self.logger.info(f"retrying in {retry_in} second(s)")
                    sleep(retry_in)

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

                    self.logger.info("result submitted")
                    break
                except (ModelTooBigException, PredictionTooBigException) as exception:
                    self.logger.error(f"failed to submit result: {exception}: {exception.size}/{exception.maximum_size} bytes")
                    sys.exit(1)
                except Exception as exception:
                    self.logger.error(f"failed to submit result: {exception}")

                    if isinstance(exception, requests.HTTPError):
                        self.logger.error(exception.response.text)
            else:
                self.logger.error("max retry reached")
                sys.exit(1)
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
            logger=self.logger,
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
            logger=self.logger,
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
                self.logger.error(f"[debug] failed to delete upload {upload.id}: {exception}")

    def do_bash(
        self,
        arguments: list[str],
        env_vars: dict[str, str | None] | None = None
    ) -> int:
        env = None
        if env_vars:
            env = os.environ.copy()
            for key, value in env_vars.items():
                if value is not None:
                    env[key] = value
                elif key in env:
                    del env[key]

        self.logger.info(f"executing command: {' '.join(arguments)}")
        process = subprocess.Popen(
            arguments,
            env=env,
            cwd=self.code_directory,
        )

        return process.wait()

    def bash(
        self,
        prefix: str,
        arguments: list[str],
        env: dict[str, str | None] | None = None
    ):
        arguments = [
            self.prefix_path,
            prefix,
            "--",
            *arguments
        ]

        code = self.do_bash(arguments, env)
        if code != 0:
            self.logger.error(f"command not exited correctly: {code}")
            sys.exit(code)

    def bash2(
        self,
        arguments: list[str],
    ):
        self.bash(arguments[0], arguments)

    def pip(
        self,
        arguments: list[str],
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
        commands: list[str],
    ):
        self.bash(
            f"recursive-{commands[0]}",
            [
                "find",
                directory_path,
                "-mindepth", "1",
                "-maxdepth", "1",
                "-exec", *commands, ";"
            ]
        )

    def delete_content(
        self,
        directory_path: str,
    ):
        """
        Deletes all content in the given directory.
        """

        self.recursive_bash(
            directory_path,
            ["rm", "-rf", "{}"],
        )

    def change_data_permissions(self, *, can_access: bool):
        mode = "a+r" if can_access else CHMOD_RESET

        self.recursive_bash(
            self.data_directory,
            ["chmod", mode, "-R", "{}"],
        )

    def sandbox(
        self,
        train: bool,
        command: str,
        parameters: KwargsLike = {},
        install_data_fuse: bool = True,
        prefix_head: int = DEFAULT_LOG_LIMITS[0],
        prefix_tail: int = DEFAULT_LOG_LIMITS[1],
    ) -> None:
        """
        The user's code can leave processes behind, which would read what the runner does next, such as the data opened by the fuse.
        They are killed when the last of the runs active together ends: the runs can be parallel, and they are all the same user.
        """

        with self._active_sandbox_count_lock:
            self._active_sandbox_count += 1

        try:
            self._sandbox(train, command, parameters, install_data_fuse, prefix_head, prefix_tail)
        finally:
            with self._active_sandbox_count_lock:
                self._active_sandbox_count -= 1

                if self._active_sandbox_count == 0:
                    exit_code = self.do_bash([self.sandbox_path, "clean"])
                    if exit_code != 0:
                        self.logger.error(f"could not stop the processes left by the user code: exit code {exit_code}")
                        sys.exit(1)

                    _collect_zombie_children()

                    for _, file_name, reason in _find_files(self.prediction_directory_path):
                        if reason is not None:
                            self.logger.error(f"unsafe file left by the user code in the prediction directory: `{file_name}` {reason}")
                            sys.exit(1)

    def _sandbox(
        self,
        train: bool,
        command: str,
        parameters: KwargsLike,
        install_data_fuse: bool,
        prefix_head: int,
        prefix_tail: int,
    ) -> None:
        (
            trace_file_path,
            exit_file_path,
            exit_content,
        ) = self._prepare_sandbox()

        if install_data_fuse:
            self._install_permission_fuse()
        else:
            self.change_data_permissions(can_access=False)

        options: KwargsLike = {
            "competition": self.competition.name,
            # ---
            "data-directory": self.data_directory,
            # ---
            "main-file": self.main_file,
            "code-directory": self.code_directory,
            "model-directory": self.model_directory_path,
            "prediction-directory": self.prediction_directory_path,
            "trace": trace_file_path,
            "ping-url": [
                urljoin(
                    store.api_base_url,
                    "/v1/runner/ping"
                ),
                "https://1.1.1.1",  # CloudFlare
            ],
            # ---
            "train": train,
            "gpu": self.gpu,
            # ---
            "fuse-pid": os.getpid() if install_data_fuse else 0,
            "fuse-signal-number": FUSE_SIGNAL.value,
            "exit-file": exit_file_path,
            "exit-content": exit_content,
            # ---
            "runner-py-file": self.runner_dot_py_file_path,
            "command": command,
            "parameters": json.dumps(parameters),
        }

        # TODO move to a dedicated function
        args: list[str] = []

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

        exit_code = self.do_bash(
            [
                self.sandbox_path, "run",
                "--network-restriction=unix-socket-only",
                *(f"--forbid-environment-variable={name}" for name in SECRET_ENVIRONMENT_VARIABLE_NAMES),
                "--",
                self.prefix_path, f"--head={prefix_head}", f"--tail={prefix_tail}", "user-code",
                "--",
                # isolated: neither the code directory nor the user site in `sys.path` before the executor loads the user's code
                "python3", "-I", "-u",
                "-m", "crunch", "runner", "cloud-executor",
                *args
            ],
            {
                **self.venv_env,
                "CRUNCH_AUTO_MONKEY_PATCH": "true",
            }
        )

        self._validate_sandbox_exit(
            exit_code,
            trace_file_path,
            exit_file_path,
            exit_content
        )

    def _prepare_sandbox(self):
        sandbox_id = str(uuid4())

        this_sandbox_directory_path = os.path.join(self.sandboxes_directory_path, sandbox_id)
        os.makedirs(this_sandbox_directory_path)  # TODO delete?
        os.chmod(this_sandbox_directory_path, MODE_TRAVERSE_ONLY_FOR_OTHERS)

        trace_file_path = os.path.join(this_sandbox_directory_path, f"trace.txt")
        exit_file_path = os.path.join(this_sandbox_directory_path, f"exit.txt")

        # created by the runner: the user's code can only write them
        for file_path in (trace_file_path, exit_file_path):
            fd = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, MODE_WRITE_ONLY_FOR_OTHERS)
            os.fchmod(fd, MODE_WRITE_ONLY_FOR_OTHERS)
            os.close(fd)

        exit_content = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))

        return (
            trace_file_path,
            exit_file_path,
            exit_content,
        )

    def _validate_sandbox_exit(
        self,
        exit_code: int,
        trace_file_path: str,
        exit_file_path: str,
        exit_content: str,
    ):
        trace_content = _read_end(trace_file_path, MAX_TRACE_SIZE)
        got_content = _read_end(exit_file_path, len(exit_content) * 2)

        if trace_content:
            self.report_error_trace(trace_content)

            # the user code can exit with 0 after an error, the run must still fail
            sys.exit(exit_code or 1)

        if got_content != exit_content:
            self.logger.error(f"[debug] failed exit check - expected=`{exit_content}` got=`{got_content}`")
            self.logger.error("user code ended prematurely, likely running out of memory or silently calling exit")

            # an early `exit(0)` from the user code ends with 0, the run must still fail
            sys.exit(exit_code or 1)

        if exit_code != 0:
            self.logger.error(f"user code exited with code {exit_code}")
            sys.exit(exit_code)

    def _install_permission_fuse(self):
        self.change_data_permissions(can_access=True)

        def on_signal(signum: int, stack: Any):
            signal.signal(FUSE_SIGNAL, signal.SIG_DFL)

            self.change_data_permissions(can_access=False)

            self.logger.info("[debug] fuse triggered")

        signal.signal(FUSE_SIGNAL, on_signal)

    @property
    def venv_env(self) -> dict[str, str | None]:
        venv_bin = os.path.join(self.venv_directory, "bin")

        return {
            "VIRTUAL_ENV": self.venv_directory,
            "PATH": f"{venv_bin}:{os.environ['PATH']}",
            "PYTHONHOME": None,
            "VIRTUAL_ENV_PROMPT": "(env) ",
        }

    def prepare_data(self):
        data_release = self.run.data

        data_files = data_release.data_files
        assert data_files is not None, "the data release of a run always has its data files"

        download_data_files(
            self.data_directory,
            data_files,
            force=False,
            logger=self.logger,
            verbose=True,
            progress_bar=False,
        )

    def report_error_trace(self, trace_content: str):
        if not self._error_reported_fuse.acquire(blocking=False):
            self.logger.info("[debug] error trace not reported (already reported by another process)")
            return

        try:
            self.run.report_error(trace=trace_content)

            self.logger.info("[debug] error trace reported")
        except BaseException as ignored:
            self.logger.error(f"[debug] ignored exception when reporting error trace: {type(ignored).__name__}({ignored})")


def _download_files(
    *,
    file_urls: dict[str, str],
    directory_path: str,
    logger: logging.Logger,
) -> LastModifications:
    paths = {
        relative_path: os.path.join(directory_path, relative_path)
        for relative_path in file_urls.keys()
    }

    download_files(
        {
            paths[relative_path]: url
            for relative_path, url in file_urls.items()
        },
        logger=logger,
        verbose=True,
        progress_bar=False,
    )

    return {
        relative_path: os.stat(path).st_mtime_ns
        for relative_path, path in paths.items()
    }


def _create_logger(log_secret: str) -> logging.Logger:
    logger = logging.Logger("crunch.runner.cloud")
    logger.addHandler(_RunnerLogHandler(log_secret))

    return logger


class _RunnerLogHandler(logging.Handler):

    def __init__(self, log_secret: str):
        super().__init__()

        self._secret_prefix = f"[{log_secret}] " if log_secret else ""

    def emit(self, record: logging.LogRecord):
        try:
            if record.levelno >= logging.ERROR:
                stream, stream_name = sys.stderr, "err"
            else:
                stream, stream_name = sys.stdout, "out"

            stream.write(f"{self._secret_prefix}<runner/{stream_name}> {self.format(record)}\n")
            stream.flush()
        except Exception:
            self.handleError(record)


def read_secret(
    name: str,
    value: str | None,
    file_path: str | None,
) -> str:
    """
    The value is given directly for the local development, or in a file by the entrypoint, which is deleted once read.

    Raises:
        ValueError: If both or none are given, or if the secret is empty.
    """

    if value is not None and file_path is not None:
        raise ValueError(f"the {name} is given both directly and in a file")

    if file_path is not None:
        with open(file_path) as fd:
            value = fd.read()

        os.unlink(file_path)

    if not value:
        raise ValueError(f"no {name}")

    return value


def _read_end(file_path: str, max_size: int) -> str:
    """
    Reads at most the last `max_size` bytes of the file, marked as cut if it is longer.
    """

    with open(file_path, "rb") as fd:
        size = fd.seek(0, os.SEEK_END)
        fd.seek(max(0, size - max_size))

        content = fd.read(max_size).decode("utf-8", errors="replace")

    if size > max_size:
        return f"(cut to its last {max_size} bytes)\n{content}"

    return content


def _collect_zombie_children():
    """
    As PID 1, the runner is the parent of the orphans killed by `sandbox clean`, and the only one able to collect them.
    """

    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return  # no child left

        if pid == 0:
            return  # the children left are alive


def _check_user_file(file_status: os.stat_result) -> str | None:
    """
    The user's code writes these files, and the runner reads and uploads them as root: a link would make it use another file, such as the hidden data.
    Returns why the file is unsafe, or None.
    """

    if stat.S_ISLNK(file_status.st_mode):
        return "is a symbolic link"

    if not stat.S_ISREG(file_status.st_mode):
        return "is not a regular file"

    if file_status.st_nlink != 1:
        return "is a hard link"

    return None


def _find_files(
    directory_path: str,
) -> Generator[tuple[str, str, str | None], None, None]:
    """
    Recursively finds all files in the given directory and yields their absolute paths, relative names, and why they are unsafe, or None.
    The directories that are symbolic links are yielded as unsafe files, not followed.
    """

    root_length = len(directory_path) + 1
    for root, directory_names, file_names in os.walk(directory_path):
        relative = root[root_length:]

        for name in [*directory_names, *file_names]:
            file_path = os.path.join(root, name)

            file_status = os.lstat(file_path)
            if stat.S_ISDIR(file_status.st_mode):
                continue

            file_name = os.path.join(relative, name)
            yield file_path, file_name, _check_user_file(file_status)


def _upload_files(
    *,
    category: str,
    directory_path: str,
    uploads: UploadedFiles,
    client: Client,
    logger: logging.Logger,
    pre_modifications: LastModifications | None = None,
) -> bool:
    """
    Uploads files from the given directory.
    If a files is already uploaded, it will be reused.

    Returns:
        True if the files have changed, False otherwise.
        Always True if `pre_modifications` is None.
    """

    files: list[tuple[str, str]] = []
    total_size = 0
    post_modifications: LastModifications = {}

    for file_path, file_name, reason in _find_files(directory_path):
        if reason is not None:
            logger.warning(f"skipping `{file_name}`: {reason}")
            continue

        stat = os.stat(file_path)
        file_size = stat.st_size

        logger.info(f"{category}: found file name=`{file_name}` size={file_size}")

        files.append((file_path, file_name))
        post_modifications[file_name] = stat.st_mtime_ns
        total_size += file_size

    if pre_modifications is not None:
        has_changed = pre_modifications != post_modifications
        logger.info(f"{category}: done walking files.len={len(files)} total_size={total_size} has_changed={has_changed}")

        if not has_changed:
            return False
    else:
        logger.info(f"{category}: done walking files.len={len(files)} total_size={total_size}")

    for file_path, file_name in files:
        if file_name in uploads:
            logger.info(f"{category}: reusing upload name=`{file_name}`")
            continue

        logger.info(f"{category}: uploading name=`{file_name}`")
        uploads[file_name] = client.uploads.send_from_file(
            path=file_path,
            name=file_name,
            max_retry=3,
            logger=logger,
        )

    return True


class CloudRunnerContext(RunnerContext):

    def __init__(
        self,
        runner: CloudRunner,
    ):
        self.runner = runner

    @property
    def started_at(self):
        return self.runner.started_at

    @property
    def timeout(self):
        timeout = self.runner.timeout
        if timeout is None:
            return None

        return timedelta(seconds=timeout)

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

    @property
    def logger(self):
        return self.runner.logger

    def execute(
        self,
        *,
        command: str,
        parameters: KwargsLike | None = None,
        trace: bool = True,
        span_hidden_parameters: list[str] | None = None,
        span_attributes: KwargsLike | None = None,
        install_data_fuse: bool = True,
        log_limits: tuple[int, int] = DEFAULT_LOG_LIMITS,
    ) -> None:
        self.logger.info(f"executing - command={command}")

        span_attributes = to_execute_span_attributes(command, parameters, span_hidden_parameters, span_attributes)
        with self.runner._span(f"execute", attributes=span_attributes, skip=not trace):  # pyright: ignore[reportPrivateUsage]
            self.runner.sandbox(
                self.runner.force_first_train,
                command,
                parameters=parameters or {},
                install_data_fuse=install_data_fuse,
                prefix_head=log_limits[0],
                prefix_tail=log_limits[1],
            )
