from datetime import datetime
from time import sleep
from time import time as current_time
from typing import Any, List, Optional, Sequence

import click

from crunch.api import Prediction, Run, RunLog, RunNotFoundException, RunStatus, Score
from crunch.command._common import get_project, reformat_datetime
from crunch.utils import ascii_table


def run_list(
    limit: Optional[int],
):
    project = get_project()
    runs = project.runs.list()
    selected_run = None # TODO project.selection.run

    reached_limit = limit is not None and len(runs) > limit

    rows: List[Sequence[Any]] = []
    for run in reversed(runs):
        if not run.success:
            selection_string = "Not Selectable"
        elif selected_run is not None and run.id == selected_run.id:
            selection_string = "Selected"
        else:
            selection_string = "Selectable"

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

    ascii_table(
        headers=["#", "Status", "Subm. #", "Duration", "Created At", "Selection"],
        values=rows,
    )

    if reached_limit:
        print(f"display: only displaying the first {limit} runs, use `--limit <n>` to show more or `--all` to show all")


def run_show(run_id: int):
    run = _get_run(run_id)

    print("Run Details:")
    print(f"  ID: {run.id}")
    print(f"  Status: {_to_enriched_status(run)}")
    print(f"  Created At: {reformat_datetime(run.started_at)}")
    print(f"  Started At: {reformat_datetime(run.started_at)}")
    print(f"  Ended At: {reformat_datetime(run.ended_at)}")
    print(f"  Exit Code: {run.exit_code}", f"(likely {run.exit_reason})" if run.exit_reason else "")
    print(f"  Error Message: {run.error_message!r}")

    error_trace = run.error_trace
    if error_trace:
        lines = error_trace.split("\n")
        print(f"  Error Trace:")
        for line in lines:
            print(f"    > {line}")

    submission = run.submission
    print("")
    print("Submission Details:")
    print(f"  Number: {submission.number}")
    print(f"  Message: {submission.message!r}")

    prediction = run.prediction
    if prediction is not None:
        print("")
        print("Prediction Details:")
        print(f"  ID: {prediction.id}")
        print(f"  Status: {_to_prediction_status(prediction)}")

        print(f"  Scores:")
        for score in prediction.scores:
            print(f"    {_format_score_value(score)}")


def run_logs(
    id: int,
    tail: Optional[int],
    follow: bool,
    debug: bool,
):
    run = _get_run(id)

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
                    print(f"{created_at} [{emitter}] running package installer")

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
            print(last_id, new_last_id)

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


def run_wait(
    id: int,
    timeout: Optional[int],
    poll_interval: int,
):
    run = _get_run(id)
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


def run_select(
    id: int,
):
    run = _get_run(id)
    run.select()


def run_terminate(
    id: int,
):
    run = _get_run(id)
    if not _is_running(run):
        print("terminate: already over")
        return

    if run.terminated:
        print("terminate: already terminated")
        return

    run.terminate()


def _is_running(run: "Run") -> bool:
    return run.status != RunStatus.COMPLETED


def _format_score_value(score: Score) -> str:
    metric = score.metric
    assert metric is not None

    unit = metric.unit
    return f"{metric.display_name}: {unit.format_value(score.value)}"


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
        return "Checking"

    if not valid:
        return "Invalid: " + (prediction.error_message or "(no error message)")

    success = prediction.success
    if success is None:
        return "Scoring"

    if not success:
        return "Failed"

    return "Successful"


def _to_enriched_status(run: Run):
    status = run.status

    if status == RunStatus.CREATED:
        return "Created"
    elif status == RunStatus.PENDING:
        return "Pending"
    elif status == RunStatus.RUNNING:
        return "Running"
    elif status == RunStatus.CLEANING:
        return "Cleaning"
    elif status == RunStatus.COMPLETED:
        prediction = run.prediction

        if prediction is not None and not prediction.valid:
            return "Bad prediction"

        if run.success:
            return "Completed Successfully"

        return "Completed with Errors"

    return str(status)
