from typing import TYPE_CHECKING

from crunch import store
from crunch.api._domain.project import ProjectComputeQuota
from crunch.command._common import get_project
from crunch.external.humanfriendly import format_size
from crunch.repository import Repository

if TYPE_CHECKING:
    from crunch.api import ProjectSubmitQuotaCountLimit


def quota(
    repository: Repository,
):
    project = get_project(repository)

    submit = project.submit_quota
    compute = project.compute_quota

    print("submit quota:")
    print(f"  code files: {format_size(submit.code_files.total_size, binary=True)}")
    print(f"  model files: {format_size(submit.model_files.total_size, binary=True)}")
    print(f"  prediction files: {format_size(submit.prediction_file.total_size, binary=True)}")

    submissions_per_day = submit.submissions_per_day
    print(f"  submissions per day: {submissions_per_day.current}/{submissions_per_day.maximum} ({_remaining_submissions_text(submissions_per_day)})")

    print()
    print("compute quota:")
    print(f"  available: {compute.available}{' (not enforced, the limit is bypassed)' if compute.bypass else ''}")
    print(f"  remaining: {compute.remaining}")
    print(f"  used: {compute.used}")
    print(f"  running: {_running_text(compute)}")

    if store.show_tips:
        print()
        print("docs:")
        print(f"  - Size quotas refer to the total size of all files per submission or prediction.")

        if compute.bypass:
            print(f"  - The compute quota is bypassed: runs can be started in parallel, even with zero remaining compute time.")
        else:
            print(f"  - A run is always given an additional 30 minutes, but it is not possible to start a run with zero remaining compute time.")

        print(f"  - The submission per day counter resets at midnight UTC.")


def _remaining_submissions_text(submissions_per_day: "ProjectSubmitQuotaCountLimit"):
    if submissions_per_day.bypass:
        return "not enforced, the limit is bypassed"

    if submissions_per_day.current > submissions_per_day.maximum:
        return "blocked for today"

    return f"{submissions_per_day.maximum - submissions_per_day.current} remaining for today"


def _running_text(compute: ProjectComputeQuota):
    if compute.bypass:
        if compute.running:
            beginning = "at least one run is running"
        else:
            beginning = "no run is currently running"

        return f"{beginning} (more can be created in parallel)"

    elif compute.running:
        return "a run is still running (wait for it to end before creating another one)"

    else:
        return "no run is currently running (one can be created)"
