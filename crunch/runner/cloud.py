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
import typing
import urllib.parse

import pandas
import requests

from .. import api, downloader, store, unstructured, utils
from .collector import MemoryPredictionCollector, PredictionCollector
from .runner import Runner
from .unstructured import RunnerContext

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
FUSE_SIGNAL = signal.SIGCONT


def link(tmp_directory: str, path: str, fake: bool = False):
    if path is None:
        return None

    tmp_path = os.path.join(tmp_directory, os.path.basename(path))

    if fake:
        # "append" just in case there is already another file
        open(tmp_path, "a").close()
    else:
        os.link(path, tmp_path)

    return tmp_path


class CloudRunner(Runner):

    def __init__(
        self,
        competition: api.Competition,
        run: api.RunnerRun,
        client: api.Client,
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
        model_directory_path: str,
        prediction_path: str,
        trace_path: str,
        exit_file_path: str,
        # ---
        log_secret: str,
        train_frequency: str,
        force_first_train: bool,
        determinism_check_enabled: bool,
        gpu: bool,
        crunch_cli_commit_hash: str,
        # ---
        max_retry: int,
        retry_seconds: int
    ):
        super().__init__(
            MemoryPredictionCollector(
                # TODO Very ugly... A plugin-like runner for unstructured competition is needed.
                write_index=(
                    competition.name in [
                        "broad-2",
                        "broad-3",
                    ]
                    or competition.format == api.CompetitionFormat.UNSTRUCTURED
                )
            ),
            competition.format,
            determinism_check_enabled
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
        self.model_directory_path = model_directory_path
        self.prediction_path = prediction_path
        self.trace_path = trace_path

        self.exit_file_path = exit_file_path
        self.exit_content = None

        self.log_secret = log_secret
        self.train_frequency = train_frequency
        self.force_first_train = force_first_train
        self.gpu = gpu
        self.crunch_cli_commit_hash = crunch_cli_commit_hash

        self.max_retry = max_retry
        self.retry_seconds = retry_seconds

        self.sandbox_restriction_flag = "--change-network-namespace" if gpu else "--filter-socket-syscalls"

    def start(self):
        self.report_current("starting")

        super().start()

        self.report_current("ending")

    def initialize(self):
        if self.competition_format == api.CompetitionFormat.UNSTRUCTURED and self.log("downloading runner..."):
            self.report_current("download runner")

            loader = unstructured.deduce_code_loader(
                self.competition.name,
                "runner",
            )

            if isinstance(loader, unstructured.GithubCodeLoader):
                source = loader.source

                self.runner_dot_py_file_path = os.path.join(self.scoring_directory, "runner.py")
                with open(self.runner_dot_py_file_path, "w") as fd:
                    fd.write(source)

                self.bash2(["chmod", "a+r", self.runner_dot_py_file_path])

                loader = unstructured.LocalCodeLoader(self.runner_dot_py_file_path)
            elif isinstance(loader, unstructured.LocalCodeLoader):
                self.runner_dot_py_file_path = loader.path

            self.runner_module = unstructured.RunnerModule.load(loader)
            if self.runner_module is None:
                raise RuntimeError("no runner is available for this competition")

        if self.log("downloading code..."):
            self.report_current("download code")

            file_urls = self.run.code
            self.download_files(file_urls, self.code_directory)

        if self.log("installing requirements..."):
            if os.path.exists(self.requirements_txt_path):
                self.report_current("install requirements")

                priority_packages = []
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
                    self.pip(priority_packages)

                self.pip([
                    *(["--no-build-isolation"] if priority_packages else []),
                    "-r",
                    self.requirements_txt_path
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
            self.bash2(["chmod", "a+r", self.state_file])

        if self.log("downloading model..."):
            self.report_current("download model")

            self.bash2(["mkdir", "-p", self.model_directory_path])

            file_urls = self.run.model
            self.have_model = len(file_urls) != 0

            self.pre_model_files_modification = self.download_files(
                file_urls,
                self.model_directory_path
            )

            self.bash2(["chmod", "-R", "o+rw", self.model_directory_path])

        return (
            self.keys,
            self.have_model
        )

    def start_timeseries(self):
        self.create_trace_file()

        return super().start_timeseries()

    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        self.report_current("process loop", moon)

        return self.sandbox(
            train,
            moon,
        )

    def start_dag(self):
        self.report_current("process dag")
        self.create_trace_file()

        return super().start_dag()

    def dag_loop(
        self,
        train: bool
    ):
        return self.sandbox(train, -1)

    def start_stream(self):
        self.create_trace_file()

        return super().start_stream()

    def stream_no_model(
        self,
    ):
        self.sandbox(
            True,
            -1,
            return_result=False,
        )

    def stream_loop(
        self,
        target_column_names: api.TargetColumnNames,
    ) -> pandas.DataFrame:
        return self.sandbox(
            False,
            target_column_names.name
        )

    def start_spatial(self):
        self.create_trace_file()

        return super().start_spatial()

    def spatial_train(
        self,
    ) -> None:
        self.sandbox(
            True,
            -1,
            return_result=False,
        )

    def spatial_loop(
        self,
        target_column_names: api.TargetColumnNames
    ) -> pandas.DataFrame:
        return self.sandbox(
            False,
            target_column_names.name
        )

    def start_unstructured(self):
        self.create_trace_file()

        if self.runner_module is None:
            raise RuntimeError("no runner is available for this competition")

        context = CloudRunnerContext(self)

        return self.runner_module.run(
            context,
            self.data_directory,
            self.model_directory_path,
        )

    def finalize(self):
        self.report_current("upload result")
        self.log("uploading result...")

        if self.prediction is not None:
            self.prediction.to_parquet(
                self.prediction_path,
                index=self.prediction_collector.is_write_index
            )
        else:
            self.prediction_collector.persist(self.prediction_path)

        self.upload_results()

    def upload_results(self):
        prediction_uploads = {}
        model_uploads = {}

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
                except (api.errors.ModelTooBigException, api.errors.PredictionTooBigException) as exception:
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
            self._delete_uploads(prediction_uploads.values())
            self._delete_uploads(model_uploads.values())

    def _upload_prediction_files(self, uploads: typing.Dict[str, str]):
        prediction_file_name = os.path.basename(self.prediction_path)
        if prediction_file_name in uploads:
            self.log(f"reusing upload for prediction")
            return

        self.log(f"uploading prediction")
        uploads[prediction_file_name] = self.client.uploads.send_from_file(
            self.prediction_path,
            prediction_file_name,
            os.path.getsize(self.prediction_path),
            max_retry=3
        )

    def _upload_model_files(self, uploads: typing.Dict[str, str]):
        model_files, model_files_size = [], 0
        post_model_files_modification = {}
        for file_path, file_name in self.find_model_files():
            stat = os.stat(file_path)
            file_size = stat.st_size

            self.log(f"found model file: {file_name} ({file_size})")

            model_files.append((file_path, file_name))
            post_model_files_modification[file_name] = stat.st_mtime_ns
            model_files_size += file_size

        has_model_changed = self.pre_model_files_modification != post_model_files_modification
        self.log(f"model file_count={len(model_files)} files_size={model_files_size} has_changed={has_model_changed}")

        if not has_model_changed:
            return False

        for file_path, file_name in model_files:
            if file_name in uploads:
                self.log(f"reusing upload for {file_name}")
                continue

            self.log(f"uploading {file_name}")
            uploads[file_name] = self.client.uploads.send_from_file(
                file_path,
                file_name,
                os.path.getsize(file_path),
                max_retry=3
            )

        return True

    def _delete_uploads(self, uploads: typing.Iterable[api.Upload]):
        for upload in uploads:
            try:
                upload.delete()
            except Exception as exception:
                self.log(f"[debug] failed to delete upload {upload.id}: {exception}", error=True)

    def teardown(self):
        self.report_current(f"shutting down")

    def log(self, message: str, important=False, error=False):
        file = sys.stderr if error else sys.stdout
        prefix = f"<runner/{file.name[4:-1]}>"

        if self.log_secret:
            print(f"[{self.log_secret}] {prefix} {message}", file=file)
        else:
            print(f"{prefix} {message}", file=file)

        return True

    def create_trace_file(self):
        self.bash2(["touch", self.prediction_path, self.trace_path])
        self.bash2(["chmod", "o+w", self.prediction_path, self.trace_path])

    def initialize_state(self):
        state = {
            "splits": [
                {
                    "key": split.key,
                    "group": split.group.name,
                }
                for split in self.splits
            ],
            "metrics": [
                metric._attrs
                for metric in self.competition.metrics
            ],
            "checks": [
                check._attrs
                for check in self.competition.checks
            ],
            "default_feature_group": self.default_feature_group,
            "features": [
                feature.to_dict()
                for feature in self.features
            ]
        }

        with open(self.state_file, "w") as fd:
            json.dump(state, fd)

    def do_bash(
        self,
        arguments: list,
        env_vars: dict = None
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
        arguments: list,
        env: dict = None
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
        arguments: list
    ):
        self.bash(arguments[0], arguments)

    def pip(
        self,
        arguments: list
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

    @typing.overload
    def sandbox(
        self,
        return_result: typing.Literal[True] = True,
        *args,
        **kwargs,
    ) -> pandas.DataFrame:
        ...

    @typing.overload
    def sandbox(
        self,
        return_result: typing.Literal[False],
        *args,
        **kwargs,
    ) -> None:
        ...

    def sandbox(
        self,
        train: bool,
        loop_key: typing.Union[int, str],
        return_result=True,
        parameters: dict = None,
    ):
        is_regular = not self.competition_format.unstructured

        try:
            self._prepare_exit()

            if is_regular:
                self.bash2(["chmod", "-R", CHMOD_RESET, self.data_directory])
                self.bash2(["chmod", "o=x", self.data_directory])

                assert self.x_path is not None

                path_options = {
                    "x": self.x_path,
                    "y": self.y_path if train else None,
                    "y-raw": self.y_raw_path,
                }

                self._install_permission_fuse()
            else:
                self.bash2(["chmod", "-R", "a+r", self.data_directory])

                path_options = {}

                self._install_permission_fuse()

            options = {
                "competition-name": self.competition.name,
                "competition-format": self.competition.format.name,
                "split-key-type": self.competition.split_key_type.name,
                # ---
                "data-directory": self.data_directory,
                **path_options,
                # ---
                "main-file": self.main_file,
                "code-directory": self.code_directory,
                "model-directory": self.model_directory_path,
                "prediction": self.prediction_path,
                "trace": self.trace_path,
                "state-file": self.state_file,
                "ping-url": [
                    urllib.parse.urljoin(
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
                "id-column-name": self.column_names.id,
                "moon-column-name": self.column_names.moon,
                "side-column-name": self.column_names.side or "",
                "input-column-name": self.column_names.input or "",
                "output-column-name": self.column_names.output or "",
                "target": [
                    (
                        target_column_names.name,
                        target_column_names.side or "",
                        target_column_names.input or "",
                        target_column_names.output or "",
                        target_column_names.file_path or ""
                    )
                    for target_column_names in self.column_names.targets
                ],
                # ---
                "write-index": self.prediction_collector.write_index,
                # ---
                "fuse-pid": os.getpid(),
                "fuse-signal-number": FUSE_SIGNAL.value,
                "exit-file": self.exit_file_path,
                "exit-content": self.exit_content,
                # ---
                "runner-py-file": self.runner_dot_py_file_path,
                "parameters": json.dumps(parameters) if parameters is not None else None,
            }

            # TODO move to a dedicated function
            args = []

            def append_value(value: typing.Any):
                if isinstance(value, tuple):
                    for x in value:
                        args.append(str(x))
                else:
                    args.append(str(value))

            for key, value in options.items():
                if value is None:
                    continue

                if isinstance(value, bool):
                    value = str(value).lower()

                if isinstance(value, list):
                    for item in value:
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
                    self.sandbox_restriction_flag,
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
            self.report_trace(loop_key)
            raise

        if return_result:
            return utils.read(self.prediction_path)

    def _prepare_exit(self):
        if not os.path.exists(self.exit_file_path):
            self.bash2(["touch", self.exit_file_path])

        if (os.stat(self.exit_file_path).st_mode & S_IWALL) != S_IWALL:
            self.bash2(["chmod", "a+w", self.exit_file_path])

        # truncate
        open(self.exit_file_path, "w").close()

        self.exit_content = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))

    def _validate_exit(self):
        self.exit_content, expected_content = None, self.exit_content

        with open(self.exit_file_path) as fd:
            got_content = fd.read()

        if got_content != expected_content:
            self.log(f"[debug] failed exit check - expected=`{expected_content}` got=`{got_content[:len(expected_content) * 2]}`", error=True)
            raise RuntimeError("user code exited prematurely")

    def _install_permission_fuse(
        self,
    ):
        def call_chmod(mode):
            self.bash2([
                "find",
                self.data_directory,
                "-mindepth", "1",
                "-maxdepth", "1",
                "-exec", "chmod", mode, "-R", "{}", ";"
            ])

        call_chmod("o+r")

        def on_signal(signum, stack):
            signal.signal(FUSE_SIGNAL, signal.SIG_DFL)

            call_chmod(CHMOD_RESET)

            self.log("[debug] fuse triggered")

        signal.signal(FUSE_SIGNAL, on_signal)

    @property
    def venv_env(self):
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
        self.column_names = data_release.column_names
        self.splits = data_release.splits
        self.features = data_release.features
        self.default_feature_group = data_release.default_feature_group
        data_files = data_release.data_files

        self.keys = sorted([
            split.key
            for split in self.splits
            if split.group == api.DataReleaseSplitGroup.TEST
        ])

        file_paths = downloader.save_all(
            downloader.prepare_all(
                self.data_directory,
                data_files,
            ),
            False,
            self.log,
            progress_bar=False,
        )

        self.x_path = file_paths.get(api.KnownData.X)
        self.y_path = file_paths.get(api.KnownData.Y)
        self.y_raw_path = file_paths.get(api.KnownData.Y_RAW)

    def download(
        self,
        url: str,
        path: str
    ):
        return utils.download(
            url,
            path,
            print=self.log,
            progress_bar=False,
        )

    def download_files(
        self,
        file_urls: dict,
        directory_path: str
    ) -> dict:
        files_modification = {}
        for relative_path, url in file_urls.items():
            path = os.path.join(directory_path, relative_path)
            self.download(url, path)

            stat = os.stat(path)
            files_modification[relative_path] = stat.st_mtime_ns

        return files_modification

    def report_current(
        self,
        work: str,
        moon: int = None
    ):
        try:
            self.run.report_current(work, moon)
        except BaseException as ignored:
            self.log(
                f"ignored exception when reporting current: {type(ignored).__name__}({ignored})",
                error=True
            )

    def report_trace(
        self,
        moon: int
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

    def find_model_files(self):
        model_root_length = len(self.model_directory_path) + 1
        for root, _, filenames in os.walk(self.model_directory_path):
            relative = root[model_root_length:]

            for filename in filenames:
                file_path = os.path.join(root, filename)
                file_name = os.path.join(relative, filename)

                yield file_path, file_name


class CloudRunnerContext(RunnerContext):

    def __init__(self, runner: CloudRunner):
        self.runner = runner

    @property
    def force_first_train(self):
        return self.runner.force_first_train

    @property
    def is_local(self):
        return False

    @property
    def is_determinism_check_enabled(self):
        return self.runner.determinism_check_enabled

    def report_determinism(self, deterministic: bool):
        self.runner.deterministic = deterministic

    def log(
        self,
        message: str,
        important=False,
        error=False
    ):
        self.runner.log(message, important=important, error=error)

    def execute(
        self,
        command,
        parameters=None,
        return_prediction=False
    ):
        self.log(f"executing - command={command}")

        result = self.runner.sandbox(
            self.runner.force_first_train,
            command,
            return_result=return_prediction != False,
            parameters=parameters or {},
        )

        if isinstance(return_prediction, PredictionCollector):
            return_prediction.append(result)
        elif return_prediction == True:
            return result
