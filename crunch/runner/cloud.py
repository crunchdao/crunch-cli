import json
import os
import subprocess
import sys
import tempfile
import time
import typing
import urllib.parse

import pandas
import requests

from .. import api, store, utils
from .runner import Runner

CONFLICTING_GPU_PACKAGES = [
    "nvidia_cublas_cu11"
]


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
        # ---
        context_directory: str,
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
        super().__init__(competition.format, determinism_check_enabled)

        self.competition = competition
        self.run = run

        self.context_directory = context_directory
        self.state_file = state_file
        self.venv_directory = venv_directory
        self.data_directory = data_directory
        self.code_directory = code_directory
        self.main_file = main_file

        self.requirements_txt_path = requirements_txt_path
        self.model_directory_path = model_directory_path
        self.prediction_path = prediction_path
        self.trace_path = trace_path

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
        if self.log("downloading code..."):
            self.report_current("download code")

            file_urls = self.run.code
            self.download_files(file_urls, self.code_directory)

        if self.log("installing requirements..."):
            if os.path.exists(self.requirements_txt_path):
                self.report_current("install requirements")

                self.pip([
                    "-r", self.requirements_txt_path
                ])
            else:
                self.log("no requirements.txt found")

        if self.log("installing crunch-cli..."):
            self.report_current("install crunch-cli")

            self.pip([
                "--upgrade",
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

            data_urls = self.get_data_urls()

            for path, url in data_urls.items():
                self.download(url, path)

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

    def finalize(self, prediction: pandas.DataFrame):
        self.report_current("upload result")
        self.log("uploading result...")

        prediction_file_name = os.path.basename(self.prediction_path)

        utils.write(
            prediction,
            self.prediction_path,
            kwargs=dict(
                index=False
            )
        )

        fds = []
        try:
            prediction_fd = open(self.prediction_path, "rb")
            fds.append(prediction_fd)

            model_files, model_files_size = [], 0
            post_model_files_modification = {}
            for file_path, file_name in self.find_model_files():
                stat = os.stat(file_path)
                file_size = stat.st_size

                self.log(f"found model file: {file_name} ({file_size})")

                fd = open(file_path, "rb")
                fds.append(fd)

                model_files.append((file_name, fd))
                post_model_files_modification[file_name] = stat.st_mtime_ns
                model_files_size += file_size

            files = [
                ("predictionFile", (prediction_file_name, prediction_fd))
            ]

            has_model_changed = self.pre_model_files_modification != post_model_files_modification
            self.log(f"model file_count={len(model_files)} files_size={model_files_size} has_changed={has_model_changed}")

            if has_model_changed:
                files.extend((
                    ("modelFiles", model_file)
                    for model_file in model_files
                ))

            # TODO Use decorator instead
            for retry in range(0, self.max_retry + 1):
                if retry != 0:
                    retry_in = self.retry_seconds * retry

                    self.report_current("wait retry")
                    self.log(f"retrying in {retry_in} second(s)")
                    time.sleep(retry_in)
                    self.report_current(f"upload result try #{retry}")

                try:
                    self.run.submit_result(
                        use_initial_model=not has_model_changed,
                        deterministic=self.deterministic,
                        files=files
                    )

                    self.log("result submitted")
                    break
                except Exception as exception:
                    self.log(f"failed to submit result: {exception}", error=True)

                    if isinstance(exception, requests.HTTPError):
                        self.log(exception.response.text, error=True)

                    for fd in fds:
                        fd.seek(0)
            else:
                self.log("max retry reached", error=True)
                exit(1)
        finally:
            for fd in fds:
                fd.close()

        return prediction

    def teardown(self):
        self.report_current(f"shutting down")

    def log(self, message: str, error=False):
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
        process = subprocess.Popen(arguments, env=env)

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
                *arguments
            ],
            self.venv_env
        )

    def sandbox(
        self,
        train: bool,
        moon: int
    ):
        with tempfile.TemporaryDirectory() as tmpdirname:
            self.bash2(["chmod", "a+rxw", tmpdirname])

            y_tmp_path = link(tmpdirname, self.y_path, fake=not train)
            x_tmp_path = link(tmpdirname, self.x_path)
            y_raw_tmp_path = link(tmpdirname, self.y_raw_path)
            orthogonalization_data_tmp_path = link(tmpdirname, self.orthogonalization_data_path)

            tmp_paths = filter(bool, [
                y_tmp_path,
                x_tmp_path,
                y_raw_tmp_path,
                orthogonalization_data_tmp_path,
            ])

            self.bash2(["chmod", "a+r", *tmp_paths])

            options = {
                "competition-name": self.competition.name,
                "competition-format": self.competition.format.name,
                # ---
                "x": x_tmp_path,
                "y": y_tmp_path,
                "y-raw": y_raw_tmp_path,
                "orthogonalization-data": orthogonalization_data_tmp_path,
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
                "moon": moon,
                "embargo": self.embargo,
                "number-of-features": self.number_of_features,
                "gpu": self.gpu,
                # ---
                "id-column-name": self.column_names.id,
                "moon-column-name": self.column_names.moon,
                "target": [
                    (target_column_names.name, target_column_names.input, target_column_names.output)
                    for target_column_names in self.column_names.targets
                ],
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

            try:
                self.do_bash(
                    [
                        "sandbox", "-v", self.sandbox_restriction_flag,
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
            except SystemExit:
                self.report_trace(moon)
                raise

        return utils.read(self.prediction_path)

    @property
    def venv_env(self):
        venv_bin = os.path.join(self.venv_directory, "bin")

        return {
            "VIRTUAL_ENV": self.venv_directory,
            "PATH": f"{venv_bin}:{os.environ['PATH']}",
            "PYTHONHOME": None,
            "VIRTUAL_ENV_PROMPT": "(env) ",
        }

    def get_data_urls(self) -> typing.Dict[str, str]:
        data_release = self.run.data

        self.embargo = data_release.embargo
        self.number_of_features = data_release.number_of_features
        self.column_names = data_release.column_names
        self.splits = data_release.splits
        self.features = data_release.features
        self.default_feature_group = data_release.default_feature_group
        data_files = data_release.data_files

        x_url = data_files.x.url
        self.x_path = os.path.join(
            self.data_directory,
            f"x.{utils.get_extension(x_url)}"
        )

        y_url = data_files.y.url
        self.y_path = os.path.join(
            self.data_directory,
            f"y.{utils.get_extension(y_url)}"
        )

        data_urls = {
            self.x_path: x_url,
            self.y_path: y_url,
        }

        self.y_raw_path = None
        if data_files.y_raw:
            y_raw_url = data_files.y_raw.url
            self.y_raw_path = os.path.join(
                self.data_directory,
                f"y_raw.{utils.get_extension(y_raw_url)}"
            )

            data_urls[self.y_raw_path] = y_raw_url

        self.orthogonalization_data_path = None
        if data_files.orthogonalization_data:
            orthogonalization_data_url = data_files.orthogonalization_data.url
            self.orthogonalization_data_path = os.path.join(
                self.data_directory,
                f"orthogonalization_data.{utils.get_extension(orthogonalization_data_url)}"
            )

            data_urls[self.orthogonalization_data_path] = orthogonalization_data_url

        self.keys = sorted([
            split.key
            for split in self.splits
            if split.group == api.DataReleaseSplitGroup.TEST
        ])

        return data_urls

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
