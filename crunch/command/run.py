import sys
from enum import IntEnum
from statistics import mean
from time import sleep
from time import time as current_time
from typing import TYPE_CHECKING, Any, Sequence

import click

from crunch import store
from crunch.api import CurrentRoundNotFoundException, InternalServerException, Prediction, ProjectReference, ProjectSelection, ProjectSelectionOrigin, Run, RunLog, RunNotFoundException, RunNotSuccessfulException, RunStatus, RuntimeOptionStatus, RuntimeType, RunTraceSpan, RunTraceSpanStatus, Score
from crunch.command._common import get_project, get_submission, reformat_datetime
from crunch.external.humanfriendly import format_size
from crunch.repository import Repository
from crunch.utils import ascii_table, format_duration, parse_datetime, shorten

if TYPE_CHECKING:
    from datetime import datetime

    from crunch.api import SubmissionIdentifierType


def run_create(
    repository: Repository,
    *,
    submission_identifier: "SubmissionIdentifierType",
    train_frequency: int | None,
    force_first_train: bool | None,
    runtime_definition_name: str,
):
    project = get_project(repository)
    submission = get_submission(project, submission_identifier)

    runtime_options = list(submission.runtime_options.iter())

    runtime_option = next((x for x in runtime_options if x.definition.name == runtime_definition_name), None)
    if runtime_option is None:
        print(f"run: runtime option not found: {runtime_definition_name}")
        print(f"run: available options are:", (', '.join(x.definition.name for x in runtime_options if x.status == RuntimeOptionStatus.AVAILABLE)))

        if store.show_tips:
            print()
            print("tips:")
            print(f"  - List available in `crunch runtime list --submission {submission.number}`.")

        raise click.Abort()

    if runtime_option.status != RuntimeOptionStatus.AVAILABLE:
        print(f"run: runtime option not available: {runtime_definition_name}")
        print(f"run: available options are: {', '.join(x.definition.name for x in runtime_options if x.status == RuntimeOptionStatus.AVAILABLE)}")

        if store.show_tips:
            print()
            print("tips:")
            print(f"  - List available in `crunch runtime list --submission {submission.number}`.")

        raise click.Abort()

    if runtime_option.quota.total_seconds() == 0:
        print(f"run: you are out of quota, you must wait until the next refresh cycle")

        if store.show_tips:
            print()
            print("tips:")
            print(f"  - See your remaining quota using `crunch quota`.")

        raise click.Abort()

    competition = project.competition
    hide_train_frequency = competition.hide_train_frequency
    hide_force_first_train = competition.hide_force_first_train

    if hide_train_frequency:
        if train_frequency is not None:
            print(f"run: train frequency is not usable in this competition")
            raise click.Abort()

        train_frequency = 0
    else:
        if train_frequency is None:
            train_frequency = 0
            print(f"run: train frequency has been set to {train_frequency} as a default, change it using `--train-frequency <n>`")

    if hide_force_first_train:
        if force_first_train is not None:
            print(f"run: force first train is not usable in this competition")
            raise click.Abort()
    else:
        has_no_model = submission.model is None

        if force_first_train is None:
            force_first_train = has_no_model
            print(f"run: force first train has been set to {force_first_train} as a default, change it using `--force-first-train`")
        elif force_first_train is False and has_no_model:
            force_first_train = True
            print(f"run: force first train has been forced to {force_first_train} as submission without a `resources/` directory must always train")

    print(f"run: creating run with submission #{submission.number}, train_frequency={train_frequency}, force_first_train={force_first_train} and runtime_definition_name={runtime_definition_name}")

    run = project.runs.create(
        submission=submission,
        train_frequency=train_frequency,
        force_first_train=force_first_train,
        runtime_definition_name=runtime_definition_name,
    )

    print(f"run: created: {run.id}")

    if store.show_tips:
        print()
        print("tips:")
        print(f"  - View the run details using `crunch run show {run.id}`.")
        print(f"  - Watch the run logs using `crunch run logs {run.id} --follow`.")


def run_list(
    repository: Repository,
    *,
    limit: int | None,
):
    project = get_project(repository)
    runs = project.runs.list()
    selection = _get_selection(project)
    primary_target = project.competition.targets.primary

    reached_limit = limit is not None and len(runs) > limit

    rows: list[Sequence[Any]] = []
    for run in reversed(runs):
        rows.append(
            (
                run.id,
                _to_enriched_status(run),
                run.submission.number,
                run.duration or "unknown",
                reformat_datetime(run.created_at),
                _format_primary_score(run.prediction),
                _to_selection_status(run, selection),
            )
        )

    if reached_limit:
        rows = rows[:limit]

    print("runs:")
    ascii_table(
        headers=["id", "status", "subm. #", "duration", "created at", primary_target.name if primary_target is not None else "score", "selection"],
        values=rows,
    )

    if reached_limit:
        print()
        print(f"pagination: only displaying the first {limit} runs, use `--limit <n>` to show more or `--all` to show all")

    if store.show_tips:
        print()
        print("tips:")
        print(f"  - To view details of a specific run, use `crunch run show <run-id>`.")
        print(f"  - To view the logs of a specific run, use `crunch run logs <run-id>`.")

        if selection is not None and selection.origin != ProjectSelectionOrigin.SELECTION:
            print(f"  - No run is explicitly selected for this round, to pick one, use `crunch run select <run-id>`.")


def run_show(
    repository: Repository,
    *,
    run_id: int,
):
    run = _get_run(repository, run_id)
    selection = _get_selection(run.project)

    print("run:")
    print(f"  id: {run.id}")
    print(f"  status: {_to_enriched_status(run)}")
    print(f"  selection: {_to_selection_status(run, selection)}")
    print(f"  created at: {reformat_datetime(run.created_at)}")
    print(f"  started at: {reformat_datetime(run.started_at)}")
    print(f"  ended at: {reformat_datetime(run.ended_at)}")
    print(f"  duration: {run.duration or '(unknown)'}")
    print(f"  exit code: {run.exit_code}", f"(likely {run.exit_reason})" if run.exit_reason else "")
    print(f"  error message: {repr(run.error_message) if run.error_message else '(none)'}")

    error_trace = run.error_trace
    if error_trace:
        lines = error_trace.split("\n")
        print(f"  error trace:")
        for line in lines:

            print(f"    > {line}")

    submission = run.submission
    print()
    print("submission:")
    print(f"  number: {submission.number}")
    print(f"  message: {submission.message!r}")

    prediction = run.prediction
    if prediction is not None:
        print()
        print("prediction:")
        print(f"  id: {prediction.id}")
        print(f"  status: {_to_prediction_status(prediction)}")

        scores = prediction.scores.list()
        if scores:
            print(f"  scores:")
            for score in scores:
                print(f"    {_format_score_value(score)}")
        elif prediction.valid is None or (prediction.valid and prediction.success is None):
            print(f"  scores: (not yet: the prediction is {_to_prediction_status(prediction)}, run this command again in a minute)")
        else:
            print(f"  scores: (none)")

    if store.show_tips:
        print()
        print("tips:")
        print(f"  - To view the logs of this run, use `crunch run logs {run.id}`.")

        if run.status != RunStatus.COMPLETED:
            print(f"  - To terminate this run, use `crunch run terminate {run.id}`.")
        elif run.success and not _is_explicitly_selected(run, selection):
            print(f"  - To select this run for the Out-of-Sample, use `crunch run select {run.id}`.")


def run_logs(
    repository: Repository,
    *,
    run_id: int,
    tail: int | None,
    follow: bool,
    debug: bool,
):
    FOLLOW_POLL_INTERVAL = 5
    FOLLOW_TRIES_AFTER_COMPLETION = 2
    UNORDERED_SERVICE_LINES = ("Created Task: ", "Changed status: PENDING", "Changed status: RUNNING")

    run = _get_run(repository, run_id)

    in_package_installer: str | None = None
    latest_created_at: "datetime | None" = None

    def _print_log(log: RunLog, debug: bool):
        nonlocal in_package_installer, latest_created_at

        log_created_at = parse_datetime(log['createdAt'])
        ordered = not (log["emitter"] == "service" and log["content"].startswith(UNORDERED_SERVICE_LINES))

        # a log can arrive late, like when restored after the run started
        out_of_order = ordered and latest_created_at is not None and log_created_at < latest_created_at
        if ordered and not out_of_order:
            latest_created_at = log_created_at

        content_prefix = "(out of order) " if out_of_order else ""
        created_at = str(log_created_at.replace(microsecond=0))
        emitter = log["emitter"]
        content = log["content"]

        if not debug:
            if emitter == "r-apt":
                current_in_package_installer = "r-apt"
            elif emitter == "pip":
                current_in_package_installer = "pip"
            else:
                current_in_package_installer = None

            if current_in_package_installer != in_package_installer:
                if current_in_package_installer:
                    print(f"{created_at} [{emitter}] running package installer (show more with --debug)")

                in_package_installer = current_in_package_installer

        if not debug and (emitter == "sandbox" or content.startswith("executing command: ") or content.startswith("[debug] ") or content.startswith("[trace] ")):
            return

        if not in_package_installer:
            print(f"{created_at} [{emitter},{'stderr' if log['error'] else 'stdout'}] {content_prefix}{content}")

    fetched_logs = run.get_logs()
    printed_ids = {line["id"] for line in fetched_logs.entries}

    logs = fetched_logs.entries
    if tail is not None:
        logs = logs[-tail:] if tail else []

    for line in logs:
        _print_log(line, debug)

    cursor = fetched_logs.cursor
    if follow and cursor is not None and _is_running(run):
        tries = FOLLOW_TRIES_AFTER_COMPLETION
        while tries > 0 and cursor is not None:
            sleep(FOLLOW_POLL_INTERVAL)

            fetched_logs = run.get_logs(cursor=cursor)
            cursor = fetched_logs.cursor

            # once archived, every log is returned again
            for line in fetched_logs.entries:
                if line["id"] not in printed_ids:
                    printed_ids.add(line["id"])
                    _print_log(line, debug)

            if _is_running(run.reload()):
                tries = FOLLOW_TRIES_AFTER_COMPLETION
            else:
                tries -= 1

    if store.show_tips:
        print()
        print("tips:")
        print(f"  - To only show the latest logs, use `crunch run logs {run.id} --tail <n>`.")

        if not debug:
            print(f"  - To show advanced logs, use `crunch run logs {run.id} --debug`.")

        if run.status != RunStatus.COMPLETED:
            print(f"  - To follow the logs in real-time, use `crunch run logs {run.id} --follow`.")


def run_traces(
    repository: Repository,
    *,
    run_id: int,
):
    run = _get_run(repository, run_id)

    traces = run.get_traces()
    if not traces.spans:
        print(f"run {run_id}: no traces (yet)")
        return

    metrics = traces.metrics
    origin = min(span.started_at for span in traces.spans)

    # name: (samples, how a step sums them up, format)
    resources = {
        "cpu": (metrics.cpu, "average", _format_percent),
        "gpu": (metrics.gpu, "average", _format_percent),
        "ram": (metrics.ram, "peak", _format_size),
        "vram": (metrics.vram, "peak", _format_size),
        "disk": (metrics.disk, "peak", _format_size),
    }

    if run.runtime_type != RuntimeType.GPU:
        del resources["gpu"], resources["vram"]

    maximums: dict[str, float] = {"cpu": 100, "gpu": 100}
    limits: dict[str, str] = {}
    if run.memory:
        maximums["ram"] = memory = run.memory * 1024 * 1024
        limits["ram"] = f"of {_format_size(memory)}"

    step_resources = [name for name in resources if name != "disk"]
    not_ended = "running" if _is_running(run) else "unfinished"

    rows: list[Sequence[Any]] = []
    for span, depth in _spans_depth_first(traces.spans):
        inside = [
            index
            for index, timestamp in enumerate(metrics.timestamp)
            if span.started_at <= timestamp and (span.ended_at is None or timestamp <= span.ended_at)
        ]

        row = [
            "  " * depth + _span_name(span) + (" (failed)" if span.status == RunTraceSpanStatus.FAILED else ""),
            format_duration(span.started_at - origin),
            format_duration(span.ended_at - span.started_at) if span.ended_at else not_ended,
        ]

        for name in step_resources:
            column, summary, format_value = resources[name]
            values = _recorded(column, inside)
            row.append(format_value((mean if summary == "average" else max)(values)) if values else "-")

        rows.append(row)

    print("spans:")
    ascii_table(
        headers=[
            "step",
            "start",
            "duration",
            *(
                f"{name} {resources[name][1]}\n{limits[name]}" if name in limits else f"{name}\n{resources[name][1]}"
                for name in step_resources
            ),
        ],
        values=rows,
    )

    for span in traces.spans:
        if span.status == RunTraceSpanStatus.FAILED:
            print(f"\n{_span_name(span)}: {span.error or 'no error message'}")

    sparklines = [
        f"  {f'{name}:':<5}  {_sparkline(values, maximums.get(name))}  peak {format_value(max(values))} {limits.get(name, '')}".rstrip()
        for name, (column, _, format_value) in resources.items()
        if (values := _recorded(column))
    ]

    if sparklines:
        print()
        print("metrics:")
        print("\n".join(sparklines))


class RunWaitExitCode(IntEnum):
    """1 and 2 are left to errors and usage errors, as for every other command."""

    SUCCEEDED = 0
    FAILED = 3
    TIMED_OUT = 4
    TERMINATED = 5


WAIT_RETRIES = 3


def run_wait(
    repository: Repository,
    *,
    run_id: int,
    timeout: int | None,
    poll_interval: int,
) -> RunWaitExitCode:
    run = _get_run(repository, run_id)
    if not _is_running(run):
        print("wait: already completed")
    else:
        print("wait: waiting for completion")

        start_time = current_time()
        while _is_still_running(run):
            sleep(poll_interval)

            if timeout is not None and (current_time() - start_time) > timeout:
                print("wait: timeout exceeded")
                break

        else:
            print("wait: completed")

    exit_code = _to_wait_exit_code(run)
    print(f"wait: run status: {exit_code.name} ({_to_enriched_status(run)})")

    if store.show_tips:
        print()
        print("tips:")

        if not timeout:
            print(f"  - To only wait for a certain amount of time, use `crunch run wait {run_id} --timeout <seconds>`.")

        print(f"  - The exit code is the run status: {', '.join(f'{code.value} = {code.name}' for code in RunWaitExitCode)}, or 1 on an error.")

    return exit_code


def _is_still_running(run: "Run") -> bool:
    """Retries through network and server errors, a run can be waited for hours."""

    from requests import RequestException

    for _ in range(WAIT_RETRIES):
        try:
            return _is_running(run.reload())
        except (RequestException, InternalServerException) as exception:
            print(f"wait: could not check the run, retrying in 10 seconds: {exception}")
            sleep(10)

    return _is_running(run.reload())  # the last attempt lets its error through


SELECTION_ORIGIN_STATUSES = {
    ProjectSelectionOrigin.SELECTION: "selected",
    ProjectSelectionOrigin.PREVIOUS_ROUND: "selected (from prev. round)",
    ProjectSelectionOrigin.LAST_RUN: "selected (last, by default)",
}


def _get_selection(project: ProjectReference) -> ProjectSelection | None:
    try:
        return project.selection
    except CurrentRoundNotFoundException:
        return None


def _to_selection_status(run: Run, selection: ProjectSelection | None) -> str:
    if selection is not None and selection.run_id == run.id:
        return SELECTION_ORIGIN_STATUSES[selection.origin]

    if not run.success:
        return "not selectable"

    return "selectable"


def _is_explicitly_selected(run: Run, selection: ProjectSelection | None) -> bool:
    return selection is not None and selection.run_id == run.id and selection.origin == ProjectSelectionOrigin.SELECTION


def _to_wait_exit_code(run: "Run") -> RunWaitExitCode:
    if _is_running(run):
        return RunWaitExitCode.TIMED_OUT

    if run.terminated:
        return RunWaitExitCode.TERMINATED

    prediction = run.prediction
    if not run.success or (prediction is not None and prediction.valid == False):
        return RunWaitExitCode.FAILED

    return RunWaitExitCode.SUCCEEDED


def run_select(
    repository: Repository,
    *,
    run_id: int,
):
    run = _get_run(repository, run_id)

    try:
        selection = run.select()
    except RunNotSuccessfulException:
        print(f"select: run {run_id} is not successful, only successful runs can be selected")

        if store.show_tips:
            print()
            print("tips:")
            print("  - To find a selectable run, use `crunch run list`.")

        raise click.Abort()

    print(f"select: run {selection.run_id} selected (submission #{selection.submission_number})")

    if store.show_tips:
        print()
        print("docs:")
        print(f"  - Selection only matters for the Out-of-Sample phase.")


def run_terminate(
    repository: Repository,
    *,
    run_id: int,
):
    run = _get_run(repository, run_id)
    if not _is_running(run):
        print("terminate: already over")
        return

    if run.terminated:
        print("terminate: already terminated")

        if store.show_tips:
            print()
            print("tips:")
            print(f"  - If the run refuse to terminate, contact the team.")

        return

    run.terminate()
    print(f"terminate: run {run_id} terminated")


def _is_running(run: "Run") -> bool:
    return run.status != RunStatus.COMPLETED


def _format_primary_score(prediction: Prediction | None) -> str:
    if prediction is None:
        return "(none)"

    primary_mean = prediction.primary_mean
    if primary_mean is None:
        return "(none)"

    primary_unit = prediction.primary_unit
    if primary_unit is None:
        return str(primary_mean)

    return primary_unit.format_value(primary_mean)


def _format_score_value(score: Score) -> str:
    metric = score.metric

    value = metric.unit.format_value(score.value) if score.value is not None else "(none)"
    return f"metric: {metric.display_name!r}: {value}"


def _get_run(repository: Repository, id: int):
    try:
        return get_project(repository).runs.get(id)
    except RunNotFoundException:
        print(f"run: not found: {id}")
        raise click.Abort()


def _to_prediction_status(prediction: Prediction | None):
    if prediction is None:
        return "(missing)"

    valid = prediction.valid
    if valid is None:
        return "checking"

    if not valid:
        return "invalid: " + (prediction.error or "(no error message)")

    success = prediction.success
    if success is None:
        return "scoring"

    if not success:
        return "failed"

    return "successful"


def _to_enriched_status(run: Run):
    status = run.status

    if status == RunStatus.CREATED:
        return "created"
    elif status == RunStatus.PENDING:
        return "pending"
    elif status == RunStatus.RUNNING:
        return "running"
    elif status == RunStatus.CLEANING:
        return "cleaning"
    elif status == RunStatus.COMPLETED:
        prediction = run.prediction

        if prediction is not None and prediction.valid == False:
            return "bad prediction"

        if run.success:
            return "completed successfully"

        return "completed with errors"

    return str(status)


def _spans_depth_first(spans: list[RunTraceSpan]) -> list[tuple[RunTraceSpan, int]]:
    children: dict[int | None, list[RunTraceSpan]] = {}
    for span in sorted(spans, key=lambda span: (span.started_at, span.id)):
        children.setdefault(span.parent_id, []).append(span)

    ordered: list[tuple[RunTraceSpan, int]] = []

    def visit(parent_id: int | None, depth: int):
        for span in children.get(parent_id, []):
            ordered.append((span, depth))
            visit(span.id, depth + 1)

    visit(None, 0)
    return ordered


def _span_name(span: RunTraceSpan) -> str:
    """The command (if any) instead of the description, then every other attribute as key=value."""

    attributes = dict(span.attributes or {})
    name = attributes.pop("command", None) or span.description

    parameters: dict[str, Any] = attributes.pop("parameters", None) or {}
    attributes = {**parameters, **attributes}  # an attribute wins over a parameter of the same name

    details = [
        f"{key}={shorten(str(value), 40)}"
        for key, value in attributes.items()
        if value is not None and value is not False
    ]

    if details:
        name += f" ({', '.join(details)})"

    return name


def _recorded(column: list[Any] | None, indexes: list[int] | None = None) -> list[Any]:
    """The samples of a column (all of them, or only those at `indexes`) that were recorded."""

    if column is None:
        return []

    values = column if indexes is None else [column[index] for index in indexes]
    return [value for value in values if value is not None]


def _format_percent(value: float) -> str:
    return f"{value:.0f}%"


def _format_size(value: int) -> str:
    return format_size(value, binary=True)


def _sparkline(values: list[float], maximum: float | None = None) -> str:
    """One character per sample, at most 60: each one the peak of its slice, scaled to `maximum` (or the highest peak)."""

    levels = "▁▂▃▄▅▆▇█"
    try:
        levels.encode(sys.stdout.encoding or "ascii")
    except (UnicodeEncodeError, LookupError):
        levels = "_.-:=+*#"  # a console that cannot show the blocks, like cp1252 on Windows

    width = min(len(values), 60)
    peaks = [max(values[index * len(values) // width:(index + 1) * len(values) // width]) for index in range(width)]
    top = maximum or max(peaks) or 1

    # min(): the machine's ram can be larger than the run's limit...
    return "".join(
        levels[round(min(peak / top, 1) * (len(levels) - 1))]
        for peak in peaks
    )
