import json
import logging
import os
import sys
import urllib.parse
from functools import cached_property
from textwrap import dedent
from types import ModuleType
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

import click
import psutil

import crunch.tester as tester
from crunch.__version__ import __version__
from crunch.api import ApiException, Client, Competition, CompetitionFormat, CompetitionMode, CrunchNotFoundException, MissingPhaseDataException, RoundIdentifierType
from crunch.command.convert import convert
from crunch.command.download import download, download_no_data_available
from crunch.command.push import push
from crunch.constants import DEFAULT_MAIN_FILE_PATH, DEFAULT_MODEL_DIRECTORY, DOT_PREDICTION_DIRECTORY
from crunch.runner import is_inside
from crunch.runner.tracing import LocalTraceExporter
from crunch.runner.types import KwargsLike
from crunch.unstructured import RunnerModule, deduce_code_loader

if TYPE_CHECKING:
    import pandas

    from crunch.api._domain.runner import RunnerRunMetric
    from crunch.runner.tracing import LocalSpan


class _Inline:

    def __init__(
        self,
        *,
        user_module: ModuleType,
        model_directory_path: str,
        logger: logging.Logger,
        has_gpu: bool = False,
    ):
        self.user_module = user_module
        self.model_directory_path = model_directory_path
        self.logger = logger
        self.has_gpu = has_gpu

        self._trace_exporter = LocalTraceExporter()

        print(f"loaded inline runner with module: {user_module}")

        from . import is_inside_runner
        if is_inside_runner:
            print(f"[warning] loading the inliner in the cloud runner is not supported, this will raise an error soon", file=sys.stderr)

        print()

        version = __version__
        print(f"cli version: {version}")

        available_ram = psutil.virtual_memory().total / (1024 ** 3)
        print(f"available ram: {available_ram:.2f} gb")

        cpu_count = psutil.cpu_count()
        print(f"available cpu: {cpu_count} core")

        print(f"----")

    @cached_property
    def _competition(self) -> Competition:
        _, project = Client.from_project()
        competition = project.competition.reload()  # pyright: ignore[reportUnknownMemberType]

        return competition

    def load_data(
        self,
        round_number: RoundIdentifierType = "@current",
        force: bool = False,
        **kwargs: KwargsLike,
    ) -> Tuple[Optional["pandas.DataFrame"], Optional["pandas.DataFrame"], Optional["pandas.DataFrame"]]:
        if self._competition.format == CompetitionFormat.STREAM:
            self.load_streams()

        try:
            (
                _,  # embargo
                _,  # number of features
                _,  # split keys
                data_directory_path,
                _,
            ) = download(
                round_number=round_number,
                force=force,
            )
        except (CrunchNotFoundException, MissingPhaseDataException):
            download_no_data_available()
            raise click.Abort()

        competition_format = self._competition.format
        if competition_format == CompetitionFormat.UNSTRUCTURED:
            module = self._runner_module
            if module is None or module.get_load_data_function(ensure=False) is None:
                self.logger.info("Please follow the competition instructions to load the data.")
                return None, None, None

            return module.load_data(
                data_directory_path=data_directory_path,
                logger=self.logger,
            )

        else:
            raise NotImplementedError(f"{competition_format.name} competition format is not supported anymore")

    def load_streams(
        self,
        **kwargs: KwargsLike,
    ) -> None:
        raise NotImplementedError("STREAM competition format is not supported anymore")

    def test(
        self,
        force_first_train: bool = True,
        train_frequency: int = 1,
        raise_abort: bool = False,
        round_number: RoundIdentifierType = "@current",
        no_determinism_check: Optional[bool] = None,
    ):
        from . import library, tester

        competition = self._competition

        self._trace_exporter.reset()

        try:
            library.scan(
                module=self.user_module,
                logger=self.logger,
            )
            self.logger.warning("")

            tester.run(
                self.user_module,
                self._runner_module,
                self.model_directory_path,
                DOT_PREDICTION_DIRECTORY,
                force_first_train,
                train_frequency,
                round_number,
                competition,
                self.has_gpu,
                no_determinism_check,
                self._trace_exporter,
            )
        except KeyboardInterrupt:
            self.logger.error(f"Cancelled!")
        except click.Abort as abort:
            self.logger.error(f"Aborted!")

            if raise_abort:
                raise abort

    def _detect_plotly(self) -> bool:
        try:
            import plotly  # pyright: ignore[reportMissingTypeStubs, reportUnusedImport]
            return True
        except ImportError:
            self.logger.error(f"`plotly` is not installed, please install it to show usage metrics")
            return False

    def show_usage(self):
        metric_objects = self._trace_exporter.metrics
        if not len(metric_objects):
            self.logger.warning("No usage metrics collected, make sure to call this method after calling `.test()`")
            return

        if self._detect_plotly():
            return _create_usage_figure(metric_objects)

    def show_timings(self):
        span_objects = list(self._trace_exporter.span_by_id.values())
        if not len(span_objects):
            self.logger.warning("No spans collected, make sure to call this method after calling `.test()`")
            return

        if self._detect_plotly():
            return _create_timeline_figure(span_objects)

    def submit(
        self,
        message: Optional[str] = None,
        model_directory_relative_path: str = "",
        include_installed_packages_version: bool = False,
        notebook_file_name: str = "notebook.ipynb",
        main_file_name: str = DEFAULT_MAIN_FILE_PATH,
        print_convert_logs: bool = False,
    ):
        if message is None:
            message = input("Message: ")

        if not model_directory_relative_path or os.path.realpath(model_directory_relative_path) == os.path.realpath("."):
            model_directory_relative_path = DEFAULT_MODEL_DIRECTORY

        try:
            from IPython.display import Markdown, display  # type: ignore
        except ImportError as error:
            print(f"submit: could not import ipython, are you running in a notebook?", file=sys.stderr)
            print(f"submit: catched error: {error}", file=sys.stderr)
            return

        try:
            from google.colab import _message  # type: ignore
            response = _message.blocking_request("get_ipynb", request="", timeout_sec=5)  # type: ignore

            if response is None:
                raise NotImplementedError(f"google.colab._message.blocking_request did not answered")

            error = response.get("error")  # type: ignore
            if error is not None:
                raise NotImplementedError(f"{error.get('type')}: {error.get('description')}")  # type: ignore

            ipynb = response.get("ipynb")  # type: ignore
            if ipynb is None:
                raise NotImplementedError(f"missing ipynb, available keys are: {list(response.keys())}")  # type: ignore

            if ipynb.get("cells") is None:  # type: ignore
                raise NotImplementedError(f"missing cells, available keys are: {list(ipynb.keys())}")  # type: ignore
        except (ImportError, NotImplementedError) as error:
            client, project = Client.from_project()
            nice_url = client.format_web_url(f"/competitions/{self._competition.name}/submit/notebook")

            encoded_message = urllib.parse.quote_plus(message)
            real_url = client.format_web_url(f"/competitions/{self._competition.name}/submit/notebook?projectName={project.name}&message={encoded_message}")

            gif_file_name = "download-and-submit-notebook.gif"
            if not self._does_create_run:
                gif_file_name = "download-and-submit-notebook-deployment.gif"

            display(Markdown(dedent(f"""
                ---

                Your work could not be submitted automatically, please do so manually:
                1. Download your Notebook from Colab
                2. Upload it to the platform
                3. Create a run to validate it

                ### >> [{nice_url}]({real_url})

                <img alt="Download and Submit Notebook" src="https://raw.githubusercontent.com/crunchdao/competitions/refs/heads/master/documentation/animations/{gif_file_name}" height="600px" />

                <br />
                <small>Error preventing submit: <code>{error}</code></small>
            """)))
            return

        files_before = set(os.listdir("."))

        try:
            return self._do_submit(
                ipynb=ipynb,
                message=message,
                model_directory_relative_path=model_directory_relative_path,
                include_installed_packages_version=include_installed_packages_version,
                notebook_file_name=notebook_file_name,
                main_file_name=main_file_name,
                print_convert_logs=print_convert_logs,
            )
        except click.Abort:
            print("aborted", file=sys.stderr)
        finally:
            files_after = set(os.listdir("."))
            new_files = files_after - files_before

            for file in new_files:
                try:
                    os.unlink(file)
                except FileNotFoundError:
                    pass

    def _do_submit(
        self,
        ipynb: Any,
        message: str,
        model_directory_relative_path: str,
        include_installed_packages_version: bool,
        notebook_file_name: str,
        main_file_name: str,
        print_convert_logs: bool,
    ):
        from IPython.display import Markdown, display  # type: ignore

        with open(notebook_file_name, "w") as fd:
            json.dump(ipynb, fd)

        try:
            convert(
                notebook_file_path=notebook_file_name,
                python_file_path=main_file_name,
                write_requirements=True,
                write_embedded_files=True,
                no_freeze=True,  # will be frozen on push
                override=True,
                verbose=print_convert_logs,
            )
        except SystemExit as error:
            if error.code != 0:
                print("conversion failed", file=sys.stderr)
                return

        try:
            submission = push(
                message=message,
                main_file_path=main_file_name,
                model_directory_relative_path=model_directory_relative_path,
                include_installed_packages_version=include_installed_packages_version,
                no_afterword=True,
                dry=False,
            )
        except ApiException as error:
            print("\n---")
            error.print_helper()
            return

        if self._does_create_run:
            gif_file_name = "create-run.gif"
            click_path = f"runs/create?submissionNumber={submission.number}"
        else:
            gif_file_name = "create-deployment.gif"
            click_path = f"submissions/{submission.number}?openDeploy-{submission.id}=true"

        client, project = Client.from_project()
        run_url = client.format_web_url(f"/competitions/{self._competition.name}/models/{project.user.login}/{project.name}/{click_path}")

        display(Markdown(dedent(f"""
            ---

            Next step is to run your submission in the cloud:

            ### >> {run_url}

            <img alt="Run in the Cloud" src="https://raw.githubusercontent.com/crunchdao/competitions/refs/heads/master/documentation/animations/{gif_file_name}" height="600px" />
        """)))

    @property
    def _does_create_run(self) -> bool:
        return self._competition.mode == CompetitionMode.OFFLINE

    @property
    def is_inside_runner(self):
        return is_inside

    @cached_property
    def _runner_module(self):
        loader = deduce_code_loader(
            competition_name=self._competition.name,
            file_name="runner",
        )

        return RunnerModule.load(loader)

    def __getattr__(self, key: str):
        import crunch

        return getattr(crunch, key)


def load(
    module_name_or_module: Union[ModuleType, str] = "__main__",
    model_directory_path: str = DEFAULT_MODEL_DIRECTORY,
):
    if isinstance(module_name_or_module, str):
        module = sys.modules[module_name_or_module]
    else:
        module = module_name_or_module

    logger = tester.install_logger()
    return _Inline(
        user_module=module,
        model_directory_path=model_directory_path,
        logger=logger,
    )


def _create_usage_figure(metric_objects: List["RunnerRunMetric"]):
    import pandas
    import plotly.graph_objects as go

    has_gpu = metric_objects[0].gpu is not None
    metrics = pandas.DataFrame([
        {
            "timestamp": metric.timestamp,
            "cpu": metric.cpu,
            "ram": metric.ram,
            "disk": metric.disk,
            "gpu": metric.gpu,
            "vram": metric.vram,
        }
        for metric in metric_objects
    ])

    LINE_COLORS = {
        "cpu": "#38bdf8",
        "ram": "#a855f7",
        "disk": "#f43f5e",
        "gpu": "#10b981",
        "vram": "#f59e0b",
    }

    metrics["timestamp"] = pandas.to_datetime(metrics["timestamp"])
    for column in ["ram", "disk", "vram"]:
        metrics[f"{column}_gb"] = metrics[column] / 1024 ** 3

    figure = go.Figure()

    figure.add_trace(go.Scatter(
        x=metrics["timestamp"],
        y=metrics["cpu"],
        name="CPU",
        line=dict(color=LINE_COLORS["cpu"], width=2),
        mode="lines",
        hovertemplate="CPU: %{y:.2f}%<extra></extra>",
        yaxis="y",
    ))

    figure.add_trace(go.Scatter(
        x=metrics["timestamp"],
        y=metrics["ram_gb"],
        name="RAM",
        line=dict(color=LINE_COLORS["ram"], width=2),
        mode="lines",
        hovertemplate="RAM: %{y:.2f} GB<extra></extra>",
        yaxis="y2",
    ))

    figure.add_trace(go.Scatter(
        x=metrics["timestamp"],
        y=metrics["disk_gb"],
        name="Disk",
        line=dict(color=LINE_COLORS["disk"], width=2),
        mode="lines",
        hovertemplate="Disk: %{y:.2f} GB<extra></extra>",
        yaxis="y3",
    ))

    if has_gpu:
        figure.add_trace(go.Scatter(
            x=metrics["timestamp"],
            y=metrics["gpu"],
            name="GPU",
            line=dict(color=LINE_COLORS["gpu"], width=2),
            mode="lines",
            hovertemplate="GPU: %{y:.2f}%<extra></extra>",
            yaxis="y",
        ))

        figure.add_trace(go.Scatter(
            x=metrics["timestamp"],
            y=metrics["vram_gb"],
            name="VRAM",
            line=dict(color=LINE_COLORS["vram"], width=2),
            mode="lines",
            hovertemplate="VRAM: %{y:.2f} GB<extra></extra>",
            yaxis="y4",
        ))

    def axis_color_style(title: str, color: str) -> dict:
        return dict(
            linecolor=color,
            tickcolor=color,
            tickfont=dict(color=color),
            title=dict(text=title, font=dict(color=color)),
        )

    figure.update_layout(
        height=400,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        hoversubplots="axis",
        hovermode="x unified",
        hoverdistance=-1,
        spikedistance=-1,
        margin=dict(t=0, l=80, r=0, b=40),

        xaxis=dict(
            tickformat="%H:%M:%S",
        ),

        # CPU / GPU (%)
        yaxis=dict(
            range=[0, 100],
            ticksuffix="%",
            side="left",
            **axis_color_style("Usage (%)", "#000"),
        ),

        # RAM (GB)
        yaxis2=dict(
            ticksuffix=" GB",
            overlaying="y",
            side="right",
            **axis_color_style("RAM (GB)", LINE_COLORS["ram"]),
        ),

        # Disk (GB)
        yaxis3=dict(
            ticksuffix=" GB",
            overlaying="y",
            anchor="free",
            side="right",
            autoshift=True,
            **axis_color_style("Disk (GB)", LINE_COLORS["disk"]),
        ),

        # VRAM (GB)
        yaxis4=(
            dict(
                ticksuffix=" GB",
                overlaying="y",
                anchor="free",
                side="right",
                autoshift=True,
                **axis_color_style("VRAM (GB)", LINE_COLORS["vram"]),
            )
            if has_gpu else {}
        ),
    )

    return figure


def _create_timeline_figure(span_objects: List["LocalSpan"]):
    import pandas
    import plotly.graph_objects as go

    from crunch.api._domain.runner import RunnerRunSpanStatus

    STATUS_COLOR = {
        RunnerRunSpanStatus.STARTED: "#dde843",
        RunnerRunSpanStatus.ENDED: "#0da54f",
        RunnerRunSpanStatus.FAILED: "#ef4343",
    }

    span_dicts = []
    for span in span_objects:
        description = span.description
        attributes = span.attributes

        if attributes is not None:
            if description == "execute" and span.attributes is not None and span.attributes.get("command") is not None:
                description = f"{description} [{span.attributes['command']}]"

            attributes = json.dumps(attributes)

        span_dicts.append({
            "id": str(span.id),
            "parent_id": str(span.parent_id) if span.parent_id is not None else "",
            "description": description,
            "started_at": pandas.to_datetime(span.started_at),
            "ended_at": pandas.to_datetime(span.ended_at),
            "duration": -1,
            "offset": -1,
            "status": span.status,
            "attributes": attributes,
            "error": span.error,
        })

    spans = pandas.DataFrame(span_dicts)
    spans["duration"] = (spans["ended_at"] - spans["started_at"]).dt.total_seconds()
    spans["offset"] = (spans["started_at"] - spans["started_at"].min()).dt.total_seconds()

    figure = go.Figure()

    for _, row in spans.iterrows():
        hovertemplate = (
            "%{customdata[0]}<br>"
            "Duration: %{customdata[1]}<br>"
        )

        duration = row["duration"]
        duration_str = f"{duration:.2f} seconds"

        attributes = row["attributes"]
        if attributes:
            hovertemplate += "Attributes: %{customdata[2]}<br>"

        error = row["error"]
        if error:
            hovertemplate += "Error: %{customdata[3]}<br>"

        figure.add_trace(go.Bar(
            orientation="h",
            x=[row["duration"]],
            y=[row["description"]],
            base=[row["offset"]],
            marker_color=STATUS_COLOR.get(row["status"], "#fff"),
            customdata=[[row["description"], duration_str, attributes, error]],
            hovertemplate=hovertemplate + "<extra></extra>",
            showlegend=False,
        ))

    figure.update_layout(
        height=max(300, len(spans) * 40),
        barmode="overlay",
        xaxis=dict(
            tickformat="%H:%M:%S",
        ),
        yaxis=dict(
            autorange="reversed",
            categoryorder="array",
            categoryarray=spans["description"].tolist(),
        ),
        margin=dict(t=20, l=150, r=20, b=0),
        hoverlabel=dict(
            bgcolor="white",
            font=dict(color="black"),
        ),
    )

    return figure
