from datetime import datetime
from time import sleep
from time import time as current_time
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

import click

from crunch.api import Prediction, Run, RunLog, RunNotFoundException, RunStatus, RuntimeOptionStatus, Score
from crunch.command._common import get_project, reformat_datetime
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType


def run_create(
    *,
    submission_identifier: "SubmissionIdentifierType",
    train_frequency: Optional[int],
    force_first_train: Optional[bool],
    runtime_definition_name: str,
    show_tips: bool = False,
):
    project = get_project()

    submission = project.submissions.get(submission_identifier)

    runtime_options = list(submission.runtime_options.list())

    runtime_option = next((x for x in runtime_options if x.definition.name == runtime_definition_name), None)
    if runtime_option is None:
        print(f"run: runtime option not found: {runtime_definition_name}")
        print(f"run: available options are:", (', '.join(x.definition.name for x in runtime_options if x.status == RuntimeOptionStatus.AVAILABLE)))

        if show_tips:
            print()
            print("tips:")
            print(f"  - List available in `crunch runtime list --submission {submission.number}`.")

        raise click.Abort()

    if runtime_option.status != RuntimeOptionStatus.AVAILABLE:
        print(f"run: runtime option not available: {runtime_definition_name}")
        print(f"run: available options are: {', '.join(x.definition.name for x in runtime_options if x.status == RuntimeOptionStatus.AVAILABLE)}")

        if show_tips:
            print()
            print("tips:")
            print(f"  - List available in `crunch runtime list --submission {submission.number}`.")

        raise click.Abort()

    if runtime_option.quota == 0:
        print(f"run: you are out of quota, you must wait until the next refresh cycle")

        if show_tips:
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

    if show_tips:
        print()
        print("tips:")
        print(f"  - View the run details using `crunch run show {run.id}`.")
        print(f"  - Watch the run logs using `crunch run logs {run.id} --follow`.")


def run_list(
    *,
    limit: Optional[int],
    show_tips: bool = False,
):
    project = get_project()
    runs = project.runs.list()
    selected_run = None  # TODO project.selection.run

    reached_limit = limit is not None and len(runs) > limit

    rows: List[Sequence[Any]] = []
    for run in reversed(runs):
        if not run.success:
            selection_string = "not selectable"
        elif selected_run is not None and run.id == selected_run.id:
            selection_string = "selected"
        else:
            selection_string = "selectable"

        rows.append(
            (
                run.id,
                _to_enriched_status(run),
                run.submission.number,
                run.duration,
                reformat_datetime(run.created_at),
                selection_string,
            )
        )

    if reached_limit:
        rows = rows[:limit]

    print("runs:")
    ascii_table(
        headers=["#", "status", "subm. #", "duration", "created at", "selection"],
        values=rows,
    )

    if reached_limit:
        print()
        print(f"pagination: only displaying the first {limit} runs, use `--limit <n>` to show more or `--all` to show all")

    if show_tips:
        print()
        print("tips:")
        print(f"  - To view details of a specific run, use `crunch run show <run-id>`.")
        print(f"  - To view the logs of a specific run, use `crunch run logs <run-id>`.")


def run_show(
    *,
    run_id: int,
    show_tips: bool = False,
):
    run = _get_run(run_id)

    print("run:")
    print(f"  id: {run.id}")
    print(f"  status: {_to_enriched_status(run)}")
    print(f"  created at: {reformat_datetime(run.started_at)}")
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

        print(f"  scores:")
        for score in prediction.scores:
            print(f"    {_format_score_value(score)}")

    if show_tips:
        print()
        print("tips:")
        print(f"  - To view the logs of this run, use `crunch run logs {run.id}`.")

        if run.status != RunStatus.COMPLETED:
            print(f"  - To terminate this run, use `crunch run terminate {run.id}`.")
        elif run.success:
            print(f"  - To select this run for the Out-of-Sample, use `crunch run select {run.id}`.")


def run_logs(
    *,
    run_id: int,
    tail: Optional[int],
    follow: bool,
    debug: bool,
    show_tips: bool = False,
):
    run = _get_run(run_id)

    in_package_installer: Optional[str] = None

    def _print_log(log: RunLog, debug: bool):
        nonlocal in_package_installer

        created_at = str(datetime.fromisoformat(log['createdAt']).replace(microsecond=0))
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
            print(f"{created_at} [{emitter},{'stderr' if log['error'] else 'stdout'}] {content}")

    logs = run.logs
    last_id = logs[-1]["id"] if logs else None

    if tail is not None:
        logs = logs[-tail:]

    for line in logs:
        _print_log(line, debug)

    if follow and _is_running(run):
        tries = 2
        while tries > 0:
            new_logs = run.logs
            new_last_id = max((line["id"] for line in new_logs), default=None)

            if new_last_id != last_id:
                for line in new_logs:
                    if last_id is None or line["id"] > last_id:
                        _print_log(line, debug)
                last_id = new_last_id

            sleep(10)
            if _is_running(run.reload()):
                tries = 2
            else:
                tries -= 1

    if show_tips:
        print()
        print("tips:")
        print(f"  - To only show the latest logs, use `crunch run logs {run.id} --tail <n>`.")

        if not debug:
            print(f"  - To show advanced logs, use `crunch run logs {run.id} --debug`.")

        if run.status != RunStatus.COMPLETED:
            print(f"  - To follow the logs in real-time, use `crunch run logs {run.id} --follow`.")


def run_wait(
    *,
    run_id: int,
    timeout: Optional[int],
    poll_interval: int,
    show_tips: bool = False,
):
    run = _get_run(run_id)
    if not _is_running(run):
        print("wait: already completed")
        return

    print("wait: waiting for completion")

    start_time = current_time()
    while _is_running(run.reload()):
        sleep(poll_interval)

        if timeout is not None and (current_time() - start_time) > timeout:
            print("wait: timeout exceeded")
            break

    else:
        print("wait: completed")

    if show_tips and not timeout:
        print()
        print("tips:")
        print(f"  - To only wait for a certain amount of time, use `crunch run wait {run_id} --timeout <seconds>`.")


def run_select(
    *,
    run_id: int,
    show_tips: bool = False,
):
    run = _get_run(run_id)
    run.select()

    print(f"select: run {run_id} selected")

    if show_tips:
        print()
        print("docs:")
        print(f"  - Selection only matters for the Out-of-Sample phase.")


def run_terminate(
    *,
    run_id: int,
    show_tips: bool = False,
):
    run = _get_run(run_id)
    if not _is_running(run):
        print("terminate: already over")
        return

    if run.terminated:
        print("terminate: already terminated")

        if show_tips:
            print()
            print("tips:")
            print(f"  - If the run refuse to terminate, contact the team.")

        return

    run.terminate()
    print(f"terminate: run {run_id} terminated")


def _is_running(run: "Run") -> bool:
    return run.status != RunStatus.COMPLETED


def _format_score_value(score: Score) -> str:
    metric = score.metric
    assert metric is not None

    unit = metric.unit
    return f"metric: {metric.display_name!r}: {unit.format_value(score.value)}"


def _get_run(id: int):
    try:
        return get_project().runs.get(id)
    except RunNotFoundException:
        print(f"run: not found: {id}")
        raise click.Abort()


def _to_prediction_status(prediction: Optional[Prediction]):
    if prediction is None:
        return "(missing)"

    valid = prediction.valid
    if valid is None:
        return "checking"

    if not valid:
        return "invalid: " + (prediction.error_message or "(no error message)")

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
